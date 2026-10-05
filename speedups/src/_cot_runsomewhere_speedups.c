/* The cot.runsomewhere value codec in C.
 *
 * The format and every rule are those of cot/runsomewhere/_values.py, which is
 * the reference: both must produce the same bytes and refuse the same input.
 * FORMAT names that format; the Python side uses this module only when it
 * matches.
 *
 * Like the reference it knows nothing of channels: a value it cannot encode
 * goes to default(value), which returns (code, inner); an extension item
 * comes back through ext_hook(code, inner).
 */
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdint.h>
#include <string.h>

#if PY_VERSION_HEX < 0x030B0000
#define PyFloat_Pack8(x, p, le) _PyFloat_Pack8((x), (unsigned char *)(p), (le))
#define PyFloat_Unpack8(p, le) _PyFloat_Unpack8((const unsigned char *)(p), (le))
#endif

#define FORMAT 1
#define MAX_DEPTH 200
#define FIXINT 0x80
#define FIXSTR 0xC0
#define EXT 'X'

/* -- encoding ---------------------------------------------------------------- */

typedef struct {
    unsigned char *data;
    size_t len, cap;
    PyObject *default_;
} writer;

static int
grow(writer *w, size_t extra)
{
    if (w->len + extra <= w->cap) return 0;
    size_t cap = w->cap ? w->cap : 256;
    while (cap < w->len + extra) cap *= 2;
    unsigned char *data = PyMem_Realloc(w->data, cap);
    if (!data) {
        PyErr_NoMemory();
        return -1;
    }
    w->data = data;
    w->cap = cap;
    return 0;
}

static int
put(writer *w, const void *src, size_t n)
{
    if (grow(w, n)) return -1;
    memcpy(w->data + w->len, src, n);
    w->len += n;
    return 0;
}

static int
put_byte(writer *w, unsigned char b)
{
    if (grow(w, 1)) return -1;
    w->data[w->len++] = b;
    return 0;
}

/* the low n bytes of v, big-endian */
static int
put_be(writer *w, uint64_t v, int n)
{
    if (grow(w, n)) return -1;
    for (int i = 0; i < n; i++) w->data[w->len++] = (unsigned char)(v >> (8 * (n - 1 - i)));
    return 0;
}

static int
put_length(writer *w, unsigned char small, unsigned char large, Py_ssize_t n)
{
    if (n < 256) return put_byte(w, small) || put_byte(w, (unsigned char)n) ? -1 : 0;
    if ((uint64_t)n > UINT32_MAX) {
        PyErr_Format(PyExc_TypeError, "too large to send: %zd bytes or items", n);
        return -1;
    }
    return put_byte(w, large) || put_be(w, (uint64_t)n, 4) ? -1 : 0;
}

static int enc(writer *w, PyObject *v, int depth);

static int
enc_str(writer *w, PyObject *v)
{
    Py_ssize_t n;
    const char *s = PyUnicode_AsUTF8AndSize(v, &n);
    PyObject *owned = NULL;
    if (!s) {
        /* lone surrogates: what surrogatepass is for */
        if (!PyErr_ExceptionMatches(PyExc_UnicodeEncodeError)) return -1;
        PyErr_Clear();
        owned = PyUnicode_AsEncodedString(v, "utf-8", "surrogatepass");
        if (!owned) return -1;
        s = PyBytes_AS_STRING(owned);
        n = PyBytes_GET_SIZE(owned);
    }
    int rc = (n < 32 ? put_byte(w, FIXSTR | (unsigned char)n) : put_length(w, 's', 'S', n))
             || put(w, s, n);
    Py_XDECREF(owned);
    return rc ? -1 : 0;
}

static PyObject *
call_with_signed(PyObject *callable, PyObject *args)
{
    PyObject *kwargs = Py_BuildValue("{sO}", "signed", Py_True), *result = NULL;
    if (callable && args && kwargs) result = PyObject_Call(callable, args, kwargs);
    Py_XDECREF(callable);
    Py_XDECREF(args);
    Py_XDECREF(kwargs);
    return result;
}

static int
enc_int(writer *w, PyObject *v)
{
    int overflow;
    long long x = PyLong_AsLongLongAndOverflow(v, &overflow);
    if (x == -1 && PyErr_Occurred()) return -1;
    if (!overflow) {
        if (x >= 0 && x < 64) return put_byte(w, FIXINT | (unsigned char)x);
        if (x >= -128 && x < 128) return put_byte(w, '1') || put_be(w, (uint64_t)x, 1) ? -1 : 0;
        if (x >= -32768 && x < 32768) return put_byte(w, '2') || put_be(w, (uint64_t)x, 2) ? -1 : 0;
        if (x >= INT32_MIN && x <= INT32_MAX) return put_byte(w, '4') || put_be(w, (uint64_t)x, 4) ? -1 : 0;
    }
    /* rare: int.to_bytes does the arithmetic, as in the reference */
    PyObject *bits = PyObject_CallMethod(v, "bit_length", NULL);
    if (!bits) return -1;
    Py_ssize_t size = (PyLong_AsSsize_t(bits) + 8) / 8;
    Py_DECREF(bits);
    PyObject *data = call_with_signed(PyObject_GetAttrString(v, "to_bytes"),
                                      Py_BuildValue("(ns)", size, "big"));
    if (!data) return -1;
    int rc = put_length(w, 'i', 'I', size) || put(w, PyBytes_AS_STRING(data), size);
    Py_DECREF(data);
    return rc ? -1 : 0;
}

static int
enc_items(writer *w, PyObject *v, unsigned char small, unsigned char large, int depth)
{
    if (put_length(w, small, large, PyObject_Length(v))) return -1;
    if (PyList_CheckExact(v) || PyTuple_CheckExact(v)) {
        /* a hook may change the list under us: hold each item while encoding */
        for (Py_ssize_t i = 0; i < PySequence_Fast_GET_SIZE(v); i++) {
            PyObject *item = PySequence_Fast_GET_ITEM(v, i);
            Py_INCREF(item);
            int rc = enc(w, item, depth + 1);
            Py_DECREF(item);
            if (rc) return -1;
        }
        return 0;
    }
    PyObject *it = PyObject_GetIter(v), *item;
    if (!it) return -1;
    while ((item = PyIter_Next(it))) {
        int rc = enc(w, item, depth + 1);
        Py_DECREF(item);
        if (rc) {
            Py_DECREF(it);
            return -1;
        }
    }
    Py_DECREF(it);
    return PyErr_Occurred() ? -1 : 0;
}

static int
enc_dict(writer *w, PyObject *v, int depth)
{
    if (put_length(w, '{', '}', PyDict_GET_SIZE(v))) return -1;
    Py_ssize_t pos = 0;
    PyObject *key, *item;
    while (PyDict_Next(v, &pos, &key, &item)) {
        Py_INCREF(key);
        Py_INCREF(item);
        int rc = enc(w, key, depth + 1) || enc(w, item, depth + 1);
        Py_DECREF(key);
        Py_DECREF(item);
        if (rc) return -1;
    }
    return 0;
}

static int
enc_ext(writer *w, PyObject *v, int depth)
{
    if (w->default_ == Py_None) {
        PyErr_Format(PyExc_TypeError, "cannot send %s values: %R", Py_TYPE(v)->tp_name, v);
        return -1;
    }
    PyObject *pair = PyObject_CallOneArg(w->default_, v);
    if (!pair) return -1;
    long code = -1;
    if (PyTuple_Check(pair) && PyTuple_GET_SIZE(pair) == 2)
        code = PyLong_AsLong(PyTuple_GET_ITEM(pair, 0));
    if (code < 0 || code > 255) {
        if (!PyErr_Occurred())
            PyErr_SetString(PyExc_TypeError, "default must return (code 0-255, value)");
        Py_DECREF(pair);
        return -1;
    }
    int rc = put_byte(w, EXT) || put_byte(w, (unsigned char)code)
             || enc(w, PyTuple_GET_ITEM(pair, 1), depth + 1);
    Py_DECREF(pair);
    return rc ? -1 : 0;
}

static int
enc(writer *w, PyObject *v, int depth)
{
    if (depth > MAX_DEPTH) {
        PyErr_SetString(PyExc_TypeError, "value nests too deeply, or contains itself");
        return -1;
    }
    PyTypeObject *t = Py_TYPE(v);
    if (t == &PyUnicode_Type) return enc_str(w, v);
    if (t == &PyLong_Type) return enc_int(w, v);
    if (t == &PyDict_Type) return enc_dict(w, v, depth);
    if (t == &PyList_Type) return enc_items(w, v, '[', ']', depth);
    if (t == &PyTuple_Type) return enc_items(w, v, '(', ')', depth);
    if (v == Py_None) return put_byte(w, 'N');
    if (v == Py_True) return put_byte(w, 'T');
    if (v == Py_False) return put_byte(w, 'F');
    if (t == &PyFloat_Type) {
        unsigned char buf[8];
        if (PyFloat_Pack8(PyFloat_AS_DOUBLE(v), (char *)buf, 0) < 0) return -1;
        return put_byte(w, 'D') || put(w, buf, 8) ? -1 : 0;
    }
    if (t == &PyBytes_Type)
        return put_length(w, 'b', 'B', PyBytes_GET_SIZE(v))
               || put(w, PyBytes_AS_STRING(v), PyBytes_GET_SIZE(v)) ? -1 : 0;
    if (t == &PySet_Type) return enc_items(w, v, '<', 'l', depth);
    if (t == &PyFrozenSet_Type) return enc_items(w, v, '>', 'g', depth);
    if (t == &PyComplex_Type) {
        unsigned char buf[16];
        if (PyFloat_Pack8(PyComplex_RealAsDouble(v), (char *)buf, 0) < 0
            || PyFloat_Pack8(PyComplex_ImagAsDouble(v), (char *)buf + 8, 0) < 0)
            return -1;
        return put_byte(w, 'C') || put(w, buf, 16) ? -1 : 0;
    }
    return enc_ext(w, v, depth);
}

static PyObject *
encode(PyObject *module, PyObject *const *args, Py_ssize_t nargs)
{
    if (nargs < 1 || nargs > 2) {
        PyErr_SetString(PyExc_TypeError, "encode(value, default=None)");
        return NULL;
    }
    writer w = {NULL, 0, 0, nargs == 2 ? args[1] : Py_None};
    PyObject *result = NULL;
    if (!enc(&w, args[0], 0)) result = PyBytes_FromStringAndSize((const char *)w.data, w.len);
    PyMem_Free(w.data);
    return result;
}

/* -- decoding ---------------------------------------------------------------- */

typedef struct {
    const unsigned char *p, *end;
    PyObject *ext_hook, *error;
    /* when not NULL, every container holding an extension item at any depth
     * is appended, innermost first, so a caller can rework those alone */
    PyObject *carriers;
    Py_ssize_t extensions;
} reader;

/* appends a container to the carriers when an extension item was read
 * since `before`; passes NULL through */
static PyObject *
carried(reader *r, Py_ssize_t before, PyObject *container)
{
    if (container && r->carriers && r->extensions != before
        && PyList_Append(r->carriers, container) < 0)
        Py_CLEAR(container);
    return container;
}

static PyObject *item(reader *r, int depth);

static int
need(reader *r, Py_ssize_t n)
{
    if (r->end - r->p < n) {
        PyErr_SetString(r->error, "truncated value");
        return -1;
    }
    return 0;
}

static uint32_t
be32(const unsigned char *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3];
}

/* the 8- or 32-bit count after a tag that has both forms */
static int
count(reader *r, int large, Py_ssize_t *n)
{
    if (large) {
        if (need(r, 4)) return -1;
        *n = be32(r->p);
        r->p += 4;
    }
    else {
        if (need(r, 1)) return -1;
        *n = *r->p++;
    }
    return 0;
}

/* turns the pending exception into the decoder's error, keeping its text */
static void
as_decode_error(reader *r, const char *prefix)
{
    PyObject *type, *value, *tb;
    PyErr_Fetch(&type, &value, &tb);
    PyErr_Format(r->error, "%s%S", prefix, value ? value : Py_None);
    Py_XDECREF(type);
    Py_XDECREF(value);
    Py_XDECREF(tb);
}

static PyObject *
text(reader *r, Py_ssize_t n)
{
    if (need(r, n)) return NULL;
    PyObject *s = PyUnicode_DecodeUTF8((const char *)r->p, n, "surrogatepass");
    if (!s && PyErr_ExceptionMatches(PyExc_UnicodeDecodeError)) as_decode_error(r, "");
    r->p += n;
    return s;
}

static PyObject *
items(reader *r, Py_ssize_t n, int depth)
{
    /* every item takes a byte at least: a count beyond the data is a lie, and
     * must not make us allocate for it */
    if (need(r, n)) return NULL;
    PyObject *list = PyList_New(n);
    if (!list) return NULL;
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject *v = item(r, depth);
        if (!v) {
            Py_DECREF(list);
            return NULL;
        }
        PyList_SET_ITEM(list, i, v);
    }
    return list;
}

static PyObject *
hashed(reader *r, PyObject *made, const char *what)
{
    if (!made && PyErr_ExceptionMatches(PyExc_TypeError)) as_decode_error(r, what);
    return made;
}

static PyObject *
tagged(reader *r, unsigned char tag, int depth)
{
    Py_ssize_t n, before = r->extensions;
    PyObject *list, *result;
    switch (tag) {
    case 'N': Py_INCREF(Py_None); return Py_None;
    case 'T': Py_INCREF(Py_True); return Py_True;
    case 'F': Py_INCREF(Py_False); return Py_False;
    case '1':
        if (need(r, 1)) return NULL;
        r->p += 1;
        return PyLong_FromLong((signed char)r->p[-1]);
    case '2':
        if (need(r, 2)) return NULL;
        r->p += 2;
        return PyLong_FromLong((int16_t)((r->p[-2] << 8) | r->p[-1]));
    case '4':
        if (need(r, 4)) return NULL;
        r->p += 4;
        return PyLong_FromLong((int32_t)be32(r->p - 4));
    case 'D':
        if (need(r, 8)) return NULL;
        r->p += 8;
        return PyFloat_FromDouble(PyFloat_Unpack8((const char *)r->p - 8, 0));
    case 'C':
        if (need(r, 16)) return NULL;
        r->p += 16;
        return PyComplex_FromDoubles(PyFloat_Unpack8((const char *)r->p - 16, 0),
                                     PyFloat_Unpack8((const char *)r->p - 8, 0));
    case 's': case 'S':
        if (count(r, tag == 'S', &n)) return NULL;
        return text(r, n);
    case 'b': case 'B':
        if (count(r, tag == 'B', &n) || need(r, n)) return NULL;
        r->p += n;
        return PyBytes_FromStringAndSize((const char *)r->p - n, n);
    case 'i': case 'I': {
        if (count(r, tag == 'I', &n) || need(r, n)) return NULL;
        PyObject *raw = PyBytes_FromStringAndSize((const char *)r->p, n);
        r->p += n;
        if (!raw) return NULL;
        return call_with_signed(PyObject_GetAttrString((PyObject *)&PyLong_Type, "from_bytes"),
                                Py_BuildValue("(Ns)", raw, "big"));
    }
    case '[': case ']':
        if (count(r, tag == ']', &n)) return NULL;
        return carried(r, before, items(r, n, depth + 1));
    case '(': case ')':
        if (count(r, tag == ')', &n) || !(list = items(r, n, depth + 1))) return NULL;
        result = PyList_AsTuple(list);
        Py_DECREF(list);
        return carried(r, before, result);
    case '<': case 'l':
        if (count(r, tag == 'l', &n) || !(list = items(r, n, depth + 1))) return NULL;
        result = hashed(r, PySet_New(list), "unhashable set member: ");
        Py_DECREF(list);
        return carried(r, before, result);
    case '>': case 'g':
        if (count(r, tag == 'g', &n) || !(list = items(r, n, depth + 1))) return NULL;
        result = hashed(r, PyFrozenSet_New(list), "unhashable set member: ");
        Py_DECREF(list);
        return carried(r, before, result);
    case '{': case '}': {
        if (count(r, tag == '}', &n) || !(list = items(r, 2 * n, depth + 1))) return NULL;
        PyObject *d = PyDict_New();
        for (Py_ssize_t i = 0; d && i < n; i++) {
            if (PyDict_SetItem(d, PyList_GET_ITEM(list, 2 * i), PyList_GET_ITEM(list, 2 * i + 1)) < 0) {
                Py_CLEAR(d);
                hashed(r, NULL, "unhashable dict key: ");
            }
        }
        Py_DECREF(list);
        return carried(r, before, d);
    }
    case EXT:
        if (r->ext_hook != Py_None) {
            if (need(r, 1)) return NULL;
            int code = *r->p++;
            PyObject *inner = item(r, depth + 1);
            if (!inner) return NULL;
            r->extensions++;
            result = PyObject_CallFunction(r->ext_hook, "iO", code, inner);
            Py_DECREF(inner);
            return result;
        }
        /* without a hook, an extension is an unknown tag */
        /* fall through */
    default:
        PyErr_Format(r->error, "unknown tag 0x%02x", tag);
        return NULL;
    }
}

static PyObject *
item(reader *r, int depth)
{
    if (depth > MAX_DEPTH) {
        PyErr_SetString(r->error, "value nests too deeply");
        return NULL;
    }
    if (need(r, 1)) return NULL;
    unsigned char tag = *r->p++;
    if ((tag & 0xC0) == FIXINT) return PyLong_FromLong(tag & 0x3F);
    if ((tag & 0xE0) == FIXSTR) return text(r, tag & 0x1F);
    return tagged(r, tag, depth);
}

static PyObject *
decode(PyObject *module, PyObject *const *args, Py_ssize_t nargs)
{
    if (nargs < 3 || nargs > 4 || (nargs == 4 && !PyList_CheckExact(args[3]))) {
        PyErr_SetString(PyExc_TypeError, "decode(data, ext_hook, error, carriers: list = None)");
        return NULL;
    }
    Py_buffer view;
    if (PyObject_GetBuffer(args[0], &view, PyBUF_SIMPLE) < 0) return NULL;
    reader r = {view.buf, (const unsigned char *)view.buf + view.len, args[1], args[2],
                nargs == 4 ? args[3] : NULL, 0};
    PyObject *v = item(&r, 0);
    if (v && r.p != r.end) {
        PyErr_Format(r.error, "%zd trailing bytes", (Py_ssize_t)(r.end - r.p));
        Py_CLEAR(v);
    }
    PyBuffer_Release(&view);
    return v;
}

/* -- module ------------------------------------------------------------------ */

static PyMethodDef methods[] = {
    {"encode", (PyCFunction)(void (*)(void))encode, METH_FASTCALL,
     "encode(value, default=None) -> bytes"},
    {"decode", (PyCFunction)(void (*)(void))decode, METH_FASTCALL,
     "decode(data, ext_hook, error, carriers=None) -> value"},
    {NULL, NULL, 0, NULL},
};

static int
exec_module(PyObject *module)
{
    return PyModule_AddIntConstant(module, "FORMAT", FORMAT) < 0
           || PyModule_AddIntConstant(module, "MAX_DEPTH", MAX_DEPTH) < 0
           /* decode takes a list to record carriers in */
           || PyModule_AddIntConstant(module, "RECORDS_CARRIERS", 1) < 0 ? -1 : 0;
}

static PyModuleDef_Slot slots[] = {
    {Py_mod_exec, exec_module},
#if PY_VERSION_HEX >= 0x030C0000
    /* stateless: safe in subinterpreters with their own GIL */
    {Py_mod_multiple_interpreters, Py_MOD_PER_INTERPRETER_GIL_SUPPORTED},
#endif
    {0, NULL},
};

static struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT, "_cot_runsomewhere_speedups",
    "The cot.runsomewhere value codec in C.", 0, methods, slots,
};

PyMODINIT_FUNC
PyInit__cot_runsomewhere_speedups(void)
{
    return PyModuleDef_Init(&module);
}
