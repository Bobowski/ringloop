/* Atomic access to the few loop fields that another thread may read or
 * write: the owner and running thread ids, the stop flag, the "posted" flag
 * of the cross-thread queue, and a handle's cancelled flag.
 *
 * Claiming the loop (compare-and-swap of the owner) acquires; giving it back
 * (store) releases, so the next owner sees everything the last one wrote.
 */
#ifndef LL_SYNC_H
#define LL_SYNC_H

static inline unsigned long ll_load_ulong(unsigned long *p) {
    return __atomic_load_n(p, __ATOMIC_ACQUIRE);
}

static inline void ll_store_ulong(unsigned long *p, unsigned long v) {
    __atomic_store_n(p, v, __ATOMIC_RELEASE);
}

static inline int ll_cas_ulong(unsigned long *p, unsigned long expect, unsigned long v) {
    return __atomic_compare_exchange_n(p, &expect, v, 0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
}

static inline int ll_load_int(int *p) {
    return __atomic_load_n(p, __ATOMIC_ACQUIRE);
}

static inline void ll_store_int(int *p, int v) {
    __atomic_store_n(p, v, __ATOMIC_RELEASE);
}

/* A counter only used to decide when to clean up: relaxed is enough. */
static inline void ll_add_ulong(unsigned long *p, unsigned long v) {
    __atomic_add_fetch(p, v, __ATOMIC_RELAXED);
}

static inline unsigned long ll_swap_ulong(unsigned long *p, unsigned long v) {
    return __atomic_exchange_n(p, v, __ATOMIC_ACQ_REL);
}

/* A number for the calling thread that no other thread of the process ever
 * gets: pthread_self() values are reused as soon as a thread exits, and
 * kernel thread ids once the pid space wraps around. */
static unsigned long ll_thread_token(void) {
    static unsigned long next;
    static __thread unsigned long token;
    if (!token)
        token = __atomic_add_fetch(&next, 1, __ATOMIC_RELAXED);
    return token;
}

#endif
