/*
 * assoc_hopscotch.h
 *
 *  Created on: Nov 16, 2015
 *      Author: udbhav
 */

#ifndef ASSOC_HOPSCOTCH_H_
#define ASSOC_HOPSCOTCH_H_

typedef uint32_t BitmapType;

void assoc_hopscotch_init(const int hashpower_init);
int assoc_hopscotch_insert(item* item, const uint32_t hv);
item *assoc_hopscotch_find(const char *key, const size_t nkey, const uint32_t hv);
int first_lsb_bit_indx(BitmapType x);
void assoc_hopscotch_delete(const char *key, const size_t nkey, const uint32_t hv);


#endif /* ASSOC_HOPSCOTCH_H_ */
