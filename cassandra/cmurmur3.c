/*
 * The majority of this code was taken from the python-smhasher library,
 * which can be found here: https://github.com/phensley/python-smhasher
 *
 * That library is under the MIT license with the following copyright:
 *
 * Copyright (c) 2011 Austin Appleby (Murmur3 routine)
 * Copyright (c) 2011 Patrick Hensley (Python wrapper, packaging)
 * Copyright DataStax (Minor modifications to match Cassandra's MM3 hashes)
 *
 */

#define PY_SSIZE_T_CLEAN 1
#include <Python.h>
#include <stdio.h>

//-----------------------------------------------------------------------------
// Platform-specific functions and macros

// Microsoft Visual Studio

#if defined(_MSC_VER)

typedef unsigned char uint8_t;
typedef unsigned long uint32_t;
typedef unsigned __int64 uint64_t;

typedef char int8_t;
typedef long int32_t;
typedef __int64 int64_t;

#define FORCE_INLINE	__forceinline

#include <stdlib.h>

#define ROTL32(x,y)	_rotl(x,y)
#define ROTL64(x,y)	_rotl64(x,y)

#define BIG_CONSTANT(x) (x)

// Other compilers

#else	// defined(_MSC_VER)

#include <stdint.h>

#define	FORCE_INLINE inline __attribute__((always_inline))

inline uint32_t rotl32 ( int32_t x, int8_t r )
{
  // cast to unsigned for logical right bitshift (to match C* MM3 implementation)
  return (x << r) | ((int32_t) (((uint32_t) x) >> (32 - r)));
}

inline int64_t rotl64 ( int64_t x, int8_t r )
{
  // cast to unsigned for logical right bitshift (to match C* MM3 implementation)
  return (x << r) | ((int64_t) (((uint64_t) x) >> (64 - r)));
}

#define ROTL32(x,y)	rotl32(x,y)
#define ROTL64(x,y)	rotl64(x,y)

#define BIG_CONSTANT(x) (x##LL)

#endif // !defined(_MSC_VER)

//-----------------------------------------------------------------------------
// Block read - if your platform needs to do endian-swapping or can only
// handle aligned reads, do the conversion here

// TODO 32bit?

FORCE_INLINE int64_t getblock ( const int64_t * p, int i )
{
  return p[i];
}

//-----------------------------------------------------------------------------
// Finalization mix - force all bits of a hash block to avalanche

FORCE_INLINE int64_t fmix ( int64_t k )
{
  // cast to unsigned for logical right bitshift (to match C* MM3 implementation)
  k ^= ((uint64_t) k) >> 33;
  k *= BIG_CONSTANT(0xff51afd7ed558ccd);
  k ^= ((uint64_t) k) >> 33;
  k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
  k ^= ((uint64_t) k) >> 33;

  return k;
}

int64_t MurmurHash3_x64_128 (const void * key, const int len,
                              const uint32_t seed)
{
  const int8_t * data = (const int8_t*)key;
  const int nblocks = len / 16;

  int64_t h1 = seed;
  int64_t h2 = seed;

  int64_t c1 = BIG_CONSTANT(0x87c37b91114253d5);
  int64_t c2 = BIG_CONSTANT(0x4cf5ad432745937f);
  int64_t k1 = 0;
  int64_t k2 = 0;

  const int64_t * blocks = (const int64_t *)(data);
  const int8_t * tail = (const int8_t*)(data + nblocks*16);

  //----------
  // body

  int i;
  for(i = 0; i < nblocks; i++)
  {
    int64_t k1 = getblock(blocks,i*2+0);
    int64_t k2 = getblock(blocks,i*2+1);

    k1 *= c1; k1  = ROTL64(k1,31); k1 *= c2; h1 ^= k1;

    h1 = ROTL64(h1,27); h1 += h2; h1 = h1*5+0x52dce729;

    k2 *= c2; k2  = ROTL64(k2,33); k2 *= c1; h2 ^= k2;

    h2 = ROTL64(h2,31); h2 += h1; h2 = h2*5+0x38495ab5;
  }

  //----------
  // tail
  switch(len & 15)
  {
    case 15: k2 ^= ((int64_t) (tail[14])) << 48;
    case 14: k2 ^= ((int64_t) (tail[13])) << 40;
    case 13: k2 ^= ((int64_t) (tail[12])) << 32;
    case 12: k2 ^= ((int64_t) (tail[11])) << 24;
    case 11: k2 ^= ((int64_t) (tail[10])) << 16;
    case 10: k2 ^= ((int64_t) (tail[ 9])) << 8;
    case  9: k2 ^= ((int64_t) (tail[ 8])) << 0;
             k2 *= c2; k2  = ROTL64(k2,33); k2 *= c1; h2 ^= k2;

    case  8: k1 ^= ((int64_t) (tail[ 7])) << 56;
    case  7: k1 ^= ((int64_t) (tail[ 6])) << 48;
    case  6: k1 ^= ((int64_t) (tail[ 5])) << 40;
    case  5: k1 ^= ((int64_t) (tail[ 4])) << 32;
    case  4: k1 ^= ((int64_t) (tail[ 3])) << 24;
    case  3: k1 ^= ((int64_t) (tail[ 2])) << 16;
    case  2: k1 ^= ((int64_t) (tail[ 1])) << 8;
    case  1: k1 ^= ((int64_t) (tail[ 0])) << 0;
             k1 *= c1; k1  = ROTL64(k1,31); k1 *= c2; h1 ^= k1;
  };

  //----------
  // finalization

  h1 ^= len; h2 ^= len;

  h1 += h2;
  h2 += h1;

  h1 = fmix(h1);
  h2 = fmix(h2);

  h1 += h2;
  h2 += h1;

  return h1;
}


struct module_state {
    PyObject *error;
};

#define GETSTATE(m) ((struct module_state*)PyModule_GetState(m))

static PyObject *
murmur3(PyObject *self, PyObject *args)
{
    const char *key;
    Py_ssize_t len;
    uint32_t seed = 0;
    int64_t result = 0;


    if (!PyArg_ParseTuple(args, "s#|I", &key, &len, &seed)) {
        return NULL;
    }

    // TODO handle x86 version?
    result = MurmurHash3_x64_128((void *)key, len, seed);
    return (PyObject *) PyLong_FromLongLong(result);
}

static PyMethodDef cmurmur3_methods[] = {
    {"murmur3", murmur3, METH_VARARGS, "Make an x64 murmur3 64-bit hash value"},
    {NULL, NULL, 0, NULL}
};

static int cmurmur3_traverse(PyObject *m, visitproc visit, void *arg) {
    Py_VISIT(GETSTATE(m)->error);
    return 0;
}

static int cmurmur3_clear(PyObject *m) {
    Py_CLEAR(GETSTATE(m)->error);
    return 0;
}

static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,
        "cmurmur3",
        NULL,
        sizeof(struct module_state),
        cmurmur3_methods,
        NULL,
        cmurmur3_traverse,
        cmurmur3_clear,
        NULL
};

#define INITERROR return NULL

PyObject *
PyInit_cmurmur3(void)

{
    PyObject *module = PyModule_Create(&moduledef);
    struct module_state *st = NULL;

    if (module == NULL)
        INITERROR;
    st = GETSTATE(module);

    st->error = PyErr_NewException("cmurmur3.Error", NULL, NULL);
    if (st->error == NULL) {
        Py_DECREF(module);
        INITERROR;
    }

    return module;
}
