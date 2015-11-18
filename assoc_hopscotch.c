/*
 * assoc_hopscotch.c
 *
 *  Created on: Nov 16, 2015
 *      Author: udbhav
 */

#include "memcached.h"
#include "assoc_hopscotch.h"

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

#define hashsize(n) ((ub4)1<<(n))
#define hashmask(n) (hashsize(n)-1)

// unsigned int hashpower = HASHPOWER_DEFAULT;

#define NEIGHBORHOOD 32

static pthread_mutex_t hash_items_counter_lock = PTHREAD_MUTEX_INITIALIZER;

/* Number of items in the hash table. */
static unsigned int hash_items = 0;


struct _Bucket {
	BitmapType bitmap; // neighborhood info bitmap
	item* it; // item details
	bool full; // is the bucket full
} __attribute__((__packed__));

typedef struct _Bucket Bucket;

static Bucket* buckets;

void assoc_hopscotch_init(const int hashtable_init) {
	printf("Entered init");
	hashpower = HASHPOWER_DEFAULT;
	if (hashtable_init) {
		hashpower = hashtable_init;
	}

	buckets = calloc(hashsize(hashpower), sizeof(Bucket));
	if (! buckets) {
		fprintf(stderr, "Failed to init hashtable.\n");
		exit(EXIT_FAILURE);
	}

	printf("Created %lu buckets\n", hashsize(hashpower));

	STATS_LOCK();
	stats.hash_power_level = hashpower;
	stats.hash_bytes = hashsize(hashpower) * sizeof(Bucket);
	STATS_UNLOCK();
}

static void find_closer_free_bucket(Bucket* free_bucket, unsigned int* free_distance) {
	Bucket* move_bucket = free_bucket - (NEIGHBORHOOD - 1);
	int move_free_dist = NEIGHBORHOOD - 1;
	for (; move_free_dist > 0; --move_free_dist) {
		BitmapType bitmap = move_bucket->bitmap;
		int move_new_free_distance = -1;
		BitmapType mask = 1;

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
				Bucket* new_free_bucket = move_bucket + move_new_free_distance;
				(free_bucket)->it = new_free_bucket->it;
				(free_bucket)->full = true;

				move_bucket->bitmap |= (1U << move_free_dist);
				move_bucket->bitmap &= ~(1U << move_new_free_distance);

				free_bucket = new_free_bucket;
				*free_distance -= move_free_dist;
			}
		}

		++move_bucket;
	}
}

/* Note: this isn't an assoc_update.  The key must not already exist to call this */
int assoc_hopscotch_insert(item *it, const uint32_t hv) {

	printf("Entered insert\n");
	const unsigned int num_buckets = hashsize(hashpower);

	// TODO: look at the expansion code
	register unsigned int start_index = hv & hashmask(hashpower);
	unsigned int free_distance = 0;

	// linear probe for the first free slot
	// TODO: find out what HASH_EMPTY etc are
	// TODO: INSERT_RANGE, end of page etc
	printf("Before loop start index %d\n", start_index);

	for (; start_index + free_distance < num_buckets; ++free_distance) {
		// found an empty bucket
		if (!buckets[start_index  + free_distance].full) { printf("Found free bucket\n"); break; }
	}

	printf("Hashed to %d at free distance %d, starting at %d ", hv, free_distance, start_index);
	fflush(stdout);

	unsigned int current_index = start_index + free_distance;
	// TODO: why do we need a loop here?
	do {
		if (free_distance < NEIGHBORHOOD) {
			buckets[current_index].it = it;
			buckets[current_index].full = true;
			buckets[start_index].bitmap |= (1U << free_distance);
			break;
		}
		find_closer_free_bucket(buckets + current_index, &free_distance);
	} while (buckets[current_index].full);

	pthread_mutex_lock(&hash_items_counter_lock);
	hash_items++;
	// TODO: look at expanding code
	pthread_mutex_unlock(&hash_items_counter_lock);

	MEMCACHED_ASSOC_INSERT(ITEM_key(it), it->nkey, hash_items);

	return 1;
}

item *assoc_hopscotch_find(const char *key, const size_t nkey, const uint32_t hv){

	register unsigned int start_bucket = hv & hashmask(hashpower);
	BitmapType hop_info = buckets[start_bucket].bitmap;
	item* it = buckets[start_bucket].it;
	printf("HopInfo for hv %u is %u\n", hv, hop_info);

	if(hop_info == 0U)
		return NULL;
	else if(1U == hop_info){
		printf("HopInfo is equal to 1\n");
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

//TODO move to utils.
int first_lsb_bit_indx(BitmapType x) {
	if(0==x)
		return -1;
	return __builtin_ffs(x)-1;
}


