/*
 * assoc_hopscotch.c
 *
 *  Created on: Nov 16, 2015
 *      Author: udbhav
 */

#include "memcached.h"
#include "debug.h"
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

typedef  unsigned long  int  ub4;   /* unsigned 4-byte quantities */
typedef  unsigned       char ub1;   /* unsigned 1-byte quantities */

/* how many powers of 2's worth of buckets we use */
unsigned int hashpower = HASHPOWER_DEFAULT;
#define hashsize(n) ((ub4)1<<(n))
#define hashmask(n) (hashsize(n)-1)

// unsigned int hashpower = HASHPOWER_DEFAULT;

#define NEIGHBORHOOD 32

/* Number of items in the hash table. */
static unsigned int hash_items = 0;

static Bucket* buckets;

void assoc_hopscotch_init(const int hashtable_init) {
	DBG_INFO(DBG_ASSOC_HOPSCOTCH, "Entered init with value %d\n", hashtable_init);
	hashpower = HASHPOWER_DEFAULT;
	if (hashtable_init) {
		hashpower = hashtable_init;
	}

	buckets = calloc(hashsize(hashpower), sizeof(Bucket));
	if (! buckets) {
		fprintf(stderr, "Failed to init hashtable.\n");
		exit(EXIT_FAILURE);
	}

	DBG_INFO(DBG_ASSOC_HOPSCOTCH, "Created %lu buckets\n", hashsize(hashpower));

	// TODO: wrap this in a ifdef
	memset(keyver_array, 0, sizeof(keyver_array));

	STATS_LOCK();
	stats.hash_power_level = hashpower;
	stats.hash_bytes = hashsize(hashpower) * sizeof(Bucket);
	STATS_UNLOCK();
}

static long index_of_bucket(Bucket* _bucket) {
	return _bucket - buckets;
}

void lock_incr_keyver(long idx) {
	incr_keyver(idx);
	DBG_INFO(DBG_ASSOC_HOPSCOTCH, "LOCK INCR: Key version of bucket %ld is %d\n",
			idx, read_keyver(idx));
}

void unlock_incr_keyver(long idx) {
	incr_keyver(idx);
	DBG_INFO(DBG_ASSOC_HOPSCOTCH, "UNLOCK INCR: Key version of bucket %ld is %d\n",
			idx, read_keyver(idx));
}

static void find_closer_free_bucket(Bucket** free_bucket, unsigned int* free_distance) {
	Bucket* move_bucket = *free_bucket - (NEIGHBORHOOD - 1);
	int move_free_dist = NEIGHBORHOOD - 1;

	for (; move_free_dist > 0; --move_free_dist) {
		BitmapType bitmap = move_bucket->bitmap;
		int move_new_free_distance = -1;
		BitmapType mask = 1;
		//DBG_INFO(DBG_ASSOC_HOPSCOTCH, "The bitmap for bucket %ld is %u\n", move_bucket - buckets, bitmap);
		int i;
		for (i=0; i<move_free_dist; ++i, mask <<= 1) {
			if (mask & bitmap) {
				move_new_free_distance = i;
				break;
			}
		}

		if (-1 != move_new_free_distance) {
			// TODO: check why this "if" is there, probably because of concurrency
			if (bitmap == move_bucket->bitmap) {
				//DBG_INFO(DBG_ASSOC_HOPSCOTCH, "Inside here\n");
				Bucket* new_free_bucket = move_bucket + move_new_free_distance;

				long idx_free = index_of_bucket(*free_bucket);
				long idx_move = index_of_bucket(move_bucket);
				long idx_new_free = index_of_bucket(new_free_bucket);

				lock_incr_keyver(idx_free);
				lock_incr_keyver(idx_move);
				// move_bucket is new_free_bucket,
				// make sure not to duplicate key version incr
				if (move_new_free_distance != 0) {
					lock_incr_keyver(idx_new_free);
				}

				(*free_bucket)->it = new_free_bucket->it;
				*free_bucket = new_free_bucket;

				move_bucket->bitmap |= (1U << move_free_dist);
				move_bucket->bitmap &= ~(1U << move_new_free_distance);

				unlock_incr_keyver(idx_free);
				unlock_incr_keyver(idx_move);
				// move_bucket is new_free_bucket,
				// make sure not to duplicate key version incr
				if (move_new_free_distance != 0) {
					unlock_incr_keyver(idx_new_free);
				}

				*free_distance -= (move_free_dist - move_new_free_distance);

				return;
			}
		}
		++move_bucket;
	}
	*free_bucket = 0;
	*free_distance = 0;
}

/* Note: this isn't an assoc_update.  The key must not already exist to call this */
int assoc_hopscotch_insert(item *it, const uint32_t hv) {

	//DBG_INFO(DBG_ASSOC_HOPSCOTCH, "Entered insert\n");
	const unsigned int num_buckets = hashsize(hashpower);

	// TODO: look at the expansion code
	register unsigned int start_index = hv & hashmask(hashpower);
	unsigned int free_distance = 0;

	// linear probe for the first free slot
	// TODO: find out what HASH_EMPTY etc are
	// TODO: INSERT_RANGE, end of page etc
	//DBG_INFO(DBG_ASSOC_HOPSCOTCH, "Before loop start index %d\n", start_index);

	for (; start_index + free_distance < num_buckets; ++free_distance) {
		// found an empty bucket
#if 0
		if (!buckets[start_index  + free_distance].full) { //DBG_INFO(DBG_ASSOC_HOPSCOTCH, "Found free bucket\n"); 
								   break; }
#endif
		if (buckets[start_index + free_distance].it == NULL)
			break;
	}

	//DBG_INFO(DBG_ASSOC_HOPSCOTCH, "Hashed to %d at free distance %d, starting at %d at address %p ", hv, free_distance, start_index, (void *)it);
	//fflush(stdout);

	Bucket* current_bucket = &buckets[start_index + free_distance];
	do {
		if (free_distance < NEIGHBORHOOD) {
			DBG_INFO(DBG_ASSOC_HOPSCOTCH, "Inside break condition %d\n", free_distance);

			long idx_start = start_index;
			long idx_curr = index_of_bucket(current_bucket);

			// start of write, key version increment
			lock_incr_keyver(idx_curr);
			if (current_bucket != &buckets[start_index]) {
				lock_incr_keyver(idx_start);
			}

			current_bucket->it = it;
			buckets[start_index].bitmap |= (1U << free_distance);

			// stats can be changed without lock, since we assume only one writer
			hash_items++;

			// end of write, key version increment
			unlock_incr_keyver(idx_curr);
			if (current_bucket != &buckets[start_index]) {
				unlock_incr_keyver(idx_start);
			}

			break;
		}
		find_closer_free_bucket(&current_bucket, &free_distance);
		//DBG_INFO(DBG_ASSOC_HOPSCOTCH, "Found a free backet at index %ld freedistance %d\n", (current_bucket - buckets), free_distance);
	} while (current_bucket != 0);

	if(current_bucket == 0)
		return 0;

	/*
	pthread_mutex_lock(&hash_items_counter_lock);
	hash_items++;
	// TODO: look at expanding code
	pthread_mutex_unlock(&hash_items_counter_lock);
	*/

	MEMCACHED_ASSOC_INSERT(ITEM_key(it), it->nkey, hash_items);

	return 1;
}

/*
item *assoc_hopscotch_find(const char *key, const size_t nkey, const uint32_t hv) {

	register unsigned int start_bucket = hv & hashmask(hashpower);
	BitmapType hop_info = buckets[start_bucket].bitmap;
	item* it = buckets[start_bucket].it;
	//DBG_INFO(DBG_ASSOC_HOPSCOTCH, "HopInfo for hv %u is %u\n", hv, hop_info);

	if(hop_info == 0U)
		return NULL;
	else if(1U == hop_info){
		//DBG_INFO(DBG_ASSOC_HOPSCOTCH, "HopInfo is equal to 1\n");
		if ((nkey == it->nkey) && (memcmp(key, ITEM_key(it), nkey) == 0))
			return it;
		else
			return NULL;
	}

	while(hop_info != 0U){
		register int i = first_lsb_bit_indx(hop_info);
		Bucket* current_element = buckets + start_bucket + i;
		if ((nkey == current_element->it->nkey) && (memcmp(key, ITEM_key(current_element->it), nkey) == 0))
				return current_element->it;
		hop_info &= ~(1U << i);
	}

	return NULL;

}
*/

static item* find_item(Bucket* start_bucket, const char* key, const size_t nkey) {

	BitmapType hop_info = start_bucket->bitmap;
	item* it = start_bucket->it;

	DBG_INFO(DBG_ASSOC_HOPSCOTCH, "HopInfo for key %s is %u\n", key, hop_info);

	if (hop_info == 0U) {
		return NULL;
	}

	item* ret = NULL;

	uint32_t vs, ve;
	long start_idx = index_of_bucket(start_bucket);

	if (hop_info == 1U) {
		DBG_INFO(DBG_ASSOC_HOPSCOTCH, "HopInfo is equal to 1\n");
		do {
			vs = read_keyver(start_idx);

			if ((nkey == it->nkey) && (memcmp(key, ITEM_key(it), nkey) == 0)) {
				ret = it;
			}

			ve = read_keyver(start_idx);
		} while ((vs & 1) || (vs != ve));

		return ret;
	}

	while(hop_info != 0U) {
		int i = first_lsb_bit_indx(hop_info);
		Bucket* current_element = buckets + start_idx + i;

		bool found = false;

		do {
			long idx = index_of_bucket(current_element);
			vs = read_keyver(idx);

			if ((nkey == current_element->it->nkey) && (memcmp(key, ITEM_key(current_element->it), nkey) == 0)) {
				ret = it;
				found = true;
			}

			ve = read_keyver(idx);
		} while ((vs & 1) || (vs != ve));

		if (found) {
			return ret;
		}

		hop_info &= ~(1U << i);
	}

	return NULL;
}

item* assoc_hopscotch_find(const char *key, const size_t nkey, const uint32_t hv) {
	unsigned int start_index = hv & hashmask(hashpower);
	uint32_t vs, ve;
	item* ret = NULL;

	do {
		vs = read_keyver(start_index);

		ret = find_item(&buckets[start_index], key, nkey);

		ve = read_keyver(start_index);
	} while ((vs & 1) || (vs != ve));

	return ret;
}

void assoc_hopscotch_delete(const char *key, const size_t nkey, const uint32_t hv){

	//DBG_INFO(DBG_ASSOC_HOPSCOTCH, "Entered hopscotch delete\n");
	register unsigned int start_bucket = hv & hashmask(hashpower);
	BitmapType hop_info = buckets[start_bucket].bitmap;
	item* it = buckets[start_bucket].it;

	if(hop_info == 0U){
		return; // hop_info is 0.
	}
	else if (hop_info == 1U) {
		if ((nkey == it->nkey) && (memcmp(key, ITEM_key(it), nkey) == 0)){
			buckets[start_bucket].it = NULL;
			buckets[start_bucket].bitmap = 0U;
			//DBG_INFO(DBG_ASSOC_HOPSCOTCH, "Deleted from index %d\n", start_bucket);
			return;
		}
	}

	while(hop_info != 0U){
		register int i = first_lsb_bit_indx(hop_info);
		Bucket* current_element = buckets + start_bucket + i;
		if ((nkey == current_element->it->nkey) && (memcmp(key, ITEM_key(current_element->it), nkey) == 0)){
			BitmapType mask = 1;
			mask <<= i;
			buckets[start_bucket].bitmap &= ~(mask);
			//DBG_INFO(DBG_ASSOC_HOPSCOTCH, "Deleted from index %d\n", start_bucket + i);
			buckets[start_bucket + i].it = NULL;
			return;
		}
		hop_info &= ~(1U << i);
	}
}


//TODO move to utils.
int first_lsb_bit_indx(BitmapType x) {
	if(0==x)
		return -1;
	return __builtin_ffs(x)-1;
}


