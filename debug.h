/**
 * @file debug.h
 * @brief Debugging module
 *        This module enables us to finer control over debug messages
 */

#ifndef __DEBUG_H__
#define __DEBUG_H__

/* Uncomment following line to enable debugging */
#define DEBUG

#ifdef DEBUG

extern int debug_level;
extern int debug_flags;

/*
 * Debug Levels
 */
enum {
	DBG_LVL_INFO = 0,
	DBG_LVL_WARN,
	DBG_LVL_ERR
};

/* Debug features modules */
#define DBG_MEMCACHED		0x1
#define DBG_ASSOC		0x2
#define DBG_ASSOC_HOPSCOTCH	0x4

#define DPRINTF(__level, __flags, ...) do {					\
	if(((__level) >= debug_level) && (debug_flags & (__flags))) 		\
		printf(__VA_ARGS__);						\
} while(0)

#define DBG_INFO(__flags, ...)	DPRINTF(DBG_LVL_INFO, __flags, __VA_ARGS__)
#define DBG_WARN(__flags, ...)	DPRINTF(DBG_LVL_WARN, __flags, __VA_ARGS__)
#define DBG_ERR(__flags, ...)	DPRINTF(DBG_LVL_ERR, __flags, __VA_ARGS__)

#else /* DEBUG */

#define DPRINTF(...)
#define DBG_INFO(...)
#define DBG_WARN(...)
#define DBG_ERR(...)

#endif /* DEBUG */

#endif /* __DEBUG_H__ */
