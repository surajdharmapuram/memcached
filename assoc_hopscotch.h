/*
 * assoc_hopscotch.h
 *
 *  Created on: Nov 16, 2015
 *      Author: udbhav
 */

#ifndef ASSOC_HOPSCOTCH_H_
#define ASSOC_HOPSCOTCH_H_

typedef uint32_t BitmapType;

struct _Bucket {
	BitmapType bitmap; // neighborhood info bitmap
	item* it; // item details
	bool full; // is the bucket full
} __attribute__((__packed__));

typedef struct _Bucket Bucket;

void assoc_hopscotch_init(const int hashpower_init);
int assoc_hopscotch_insert(item* item, const uint32_t hv);
item *assoc_hopscotch_find(const char *key, const size_t nkey, const uint32_t hv);
int first_lsb_bit_indx(BitmapType x);
void assoc_hopscotch_delete(const char *key, const size_t nkey, const uint32_t hv);
void unlock_incr_keyver(long idx);
void lock_incr_keyver(long idx);


#endif /* ASSOC_HOPSCOTCH_H_ */
