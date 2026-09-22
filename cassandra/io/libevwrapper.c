#include <Python.h>
#include <ev.h>

typedef struct libevwrapper_Loop {
    PyObject_HEAD
    struct ev_loop *loop;
} libevwrapper_Loop;

static void
Loop_dealloc(libevwrapper_Loop *self) {
    ev_loop_destroy(self->loop);
    Py_TYPE(self)->tp_free((PyObject *)self);
};

static PyObject*
Loop_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    libevwrapper_Loop *self;

    self = (libevwrapper_Loop *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->loop = ev_loop_new(EVBACKEND_SELECT);
        if (!self->loop) {
            PyErr_SetString(PyExc_Exception, "Error getting new ev loop");
            Py_DECREF(self);
            return NULL;
        }
    }
    return (PyObject *)self;
};

static int
Loop_init(libevwrapper_Loop *self, PyObject *args, PyObject *kwds) {
    if (!PyArg_ParseTuple(args, "")) {
        PyErr_SetString(PyExc_TypeError, "Loop.__init__() takes no arguments");
        return -1;
    }
    return 0;
};

static PyObject *
Loop_start(libevwrapper_Loop *self, PyObject *args) {
    Py_BEGIN_ALLOW_THREADS
    ev_run(self->loop, 0);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
};

static PyObject *
Loop_unref(libevwrapper_Loop *self, PyObject *args) {
    ev_unref(self->loop);
    Py_RETURN_NONE;
}

static PyMethodDef Loop_methods[] = {
    {"start", (PyCFunction)Loop_start, METH_NOARGS, "Start the event loop"},
    {"unref", (PyCFunction)Loop_unref, METH_NOARGS, "Unrefrence the event loop"},
    {NULL} /* Sentinel */
};

static
PyTypeObject libevwrapper_LoopType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "cassandra.io.libevwrapper.Loop",/*tp_name*/
    sizeof(libevwrapper_Loop),       /*tp_basicsize*/
    0,                               /*tp_itemsize*/
    (destructor)Loop_dealloc,        /*tp_dealloc*/
    0,                               /*tp_print*/
    0,                               /*tp_getattr*/
    0,                               /*tp_setattr*/
    0,                               /*tp_compare*/
    0,                               /*tp_repr*/
    0,                               /*tp_as_number*/
    0,                               /*tp_as_sequence*/
    0,                               /*tp_as_mapping*/
    0,                               /*tp_hash */
    0,                               /*tp_call*/
    0,                               /*tp_str*/
    0,                               /*tp_getattro*/
    0,                               /*tp_setattro*/
    0,                               /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    "Loop objects",                  /* tp_doc */
    0,                               /* tp_traverse */
    0,                               /* tp_clear */
    0,                               /* tp_richcompare */
    0,                               /* tp_weaklistoffset */
    0,                               /* tp_iter */
    0,                               /* tp_iternext */
    Loop_methods,                    /* tp_methods */
    0,                               /* tp_members */
    0,                               /* tp_getset */
    0,                               /* tp_base */
    0,                               /* tp_dict */
    0,                               /* tp_descr_get */
    0,                               /* tp_descr_set */
    0,                               /* tp_dictoffset */
    (initproc)Loop_init,             /* tp_init */
    0,                               /* tp_alloc */
    Loop_new,                        /* tp_new */
};

typedef struct libevwrapper_IO {
    PyObject_HEAD
    struct ev_io io;
    struct libevwrapper_Loop *loop;
    PyObject *callback;
} libevwrapper_IO;

static void
IO_dealloc(libevwrapper_IO *self) {
    Py_XDECREF(self->loop);
    Py_XDECREF(self->callback);
    Py_TYPE(self)->tp_free((PyObject *)self);
};

static void io_callback(struct ev_loop *loop, ev_io *watcher, int revents) {
    if (!Py_IsInitialized()) {
        return;
    }

    libevwrapper_IO *self = watcher->data;
    PyObject *result;
    PyGILState_STATE gstate = PyGILState_Ensure();
    if (revents & EV_ERROR && errno) {
        result = PyObject_CallFunction(self->callback, "Obi", self, revents, errno);
    } else {
        result = PyObject_CallFunction(self->callback, "Ob", self, revents);
    }
    if (!result) {
        PyErr_WriteUnraisable(self->callback);
    }
    Py_XDECREF(result);
    PyGILState_Release(gstate);
};

static int
IO_init(libevwrapper_IO *self, PyObject *args, PyObject *kwds) {
    PyObject *socket;
    PyObject *callback;
    PyObject *loop;
    int io_flags = 0, fd = -1;
    struct ev_io *io = NULL;

    if (!PyArg_ParseTuple(args, "OiOO", &socket, &io_flags, &loop, &callback)) {
        return -1;
    }

    if (loop) {
        Py_INCREF(loop);
        self->loop = (libevwrapper_Loop *)loop;
    }

    if (callback) {
        if (!PyCallable_Check(callback)) {
            PyErr_SetString(PyExc_TypeError, "callback parameter must be callable");
            Py_XDECREF(loop);
            return -1;
        }
        Py_INCREF(callback);
        self->callback = callback;
    }

    fd = PyObject_AsFileDescriptor(socket);
    if (fd == -1) {
        PyErr_SetString(PyExc_TypeError, "unable to get file descriptor from socket");
        Py_XDECREF(callback);
        Py_XDECREF(loop);
        return -1;
    }
    io = &(self->io);
    ev_io_init(io, io_callback, fd, io_flags);
    self->io.data = self;
    return 0;
}

static PyObject*
IO_start(libevwrapper_IO *self, PyObject *args) {
    ev_io_start(self->loop->loop, &self->io);
    Py_RETURN_NONE;
}

static PyObject*
IO_stop(libevwrapper_IO *self, PyObject *args) {
    ev_io_stop(self->loop->loop, &self->io);
    Py_RETURN_NONE;
}

static PyObject*
IO_is_active(libevwrapper_IO *self, PyObject *args) {
    struct ev_io *io = &(self->io);
    return PyBool_FromLong(ev_is_active(io));
}

static PyObject*
IO_is_pending(libevwrapper_IO *self, PyObject *args) {
    struct ev_io *io = &(self->io);
    return PyBool_FromLong(ev_is_pending(io));
}

static PyMethodDef IO_methods[] = {
    {"start", (PyCFunction)IO_start, METH_NOARGS, "Start the watcher"},
    {"stop", (PyCFunction)IO_stop, METH_NOARGS, "Stop the watcher"},
    {"is_active", (PyCFunction)IO_is_active, METH_NOARGS, "Is the watcher active?"},
    {"is_pending", (PyCFunction)IO_is_pending, METH_NOARGS, "Is the watcher pending?"},
    {NULL}  /* Sentinal */
};

static PyTypeObject libevwrapper_IOType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "cassandra.io.libevwrapper.IO",  /*tp_name*/
    sizeof(libevwrapper_IO),         /*tp_basicsize*/
    0,                               /*tp_itemsize*/
    (destructor)IO_dealloc,          /*tp_dealloc*/
    0,                               /*tp_print*/
    0,                               /*tp_getattr*/
    0,                               /*tp_setattr*/
    0,                               /*tp_compare*/
    0,                               /*tp_repr*/
    0,                               /*tp_as_number*/
    0,                               /*tp_as_sequence*/
    0,                               /*tp_as_mapping*/
    0,                               /*tp_hash */
    0,                               /*tp_call*/
    0,                               /*tp_str*/
    0,                               /*tp_getattro*/
    0,                               /*tp_setattro*/
    0,                               /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    "IO objects",                    /* tp_doc */
    0,                               /* tp_traverse */
    0,                               /* tp_clear */
    0,                               /* tp_richcompare */
    0,                               /* tp_weaklistoffset */
    0,                               /* tp_iter */
    0,                               /* tp_iternext */
    IO_methods,                      /* tp_methods */
    0,                               /* tp_members */
    0,                               /* tp_getset */
    0,                               /* tp_base */
    0,                               /* tp_dict */
    0,                               /* tp_descr_get */
    0,                               /* tp_descr_set */
    0,                               /* tp_dictoffset */
    (initproc)IO_init,               /* tp_init */
};

typedef struct libevwrapper_Async {
    PyObject_HEAD
    struct ev_async async;
    struct libevwrapper_Loop *loop;
} libevwrapper_Async;

static void
Async_dealloc(libevwrapper_Async *self) {
    Py_XDECREF(self->loop);
    Py_TYPE(self)->tp_free((PyObject *)self);
};

static void async_callback(EV_P_ ev_async *watcher, int revents) {};

static int
Async_init(libevwrapper_Async *self, PyObject *args, PyObject *kwds) {
    PyObject *loop;
    static char *kwlist[] = {"loop", NULL};
    struct ev_async *async = NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O", kwlist, &loop)) {
        PyErr_SetString(PyExc_TypeError, "unable to get file descriptor from socket");
        return -1;
    }

    if (loop) {
        Py_INCREF(loop);
        self->loop = (libevwrapper_Loop *)loop;
    } else {
        return -1;
    }
    async = &(self->async);
    ev_async_init(async, async_callback);
    return 0;
};

static PyObject *
Async_start(libevwrapper_Async *self, PyObject *args) {
    ev_async_start(self->loop->loop, &self->async);
    Py_RETURN_NONE;
}

static PyObject *
Async_send(libevwrapper_Async *self, PyObject *args) {
    ev_async_send(self->loop->loop, &self->async);
    Py_RETURN_NONE;
};

static PyMethodDef Async_methods[] = {
    {"start", (PyCFunction)Async_start, METH_NOARGS, "Start the watcher"},
    {"send", (PyCFunction)Async_send, METH_NOARGS, "Notify the event loop"},
    {NULL} /* Sentinel */
};

static PyTypeObject libevwrapper_AsyncType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "cassandra.io.libevwrapper.Async", /*tp_name*/
    sizeof(libevwrapper_Async),      /*tp_basicsize*/
    0,                               /*tp_itemsize*/
    (destructor)Async_dealloc,       /*tp_dealloc*/
    0,                               /*tp_print*/
    0,                               /*tp_getattr*/
    0,                               /*tp_setattr*/
    0,                               /*tp_compare*/
    0,                               /*tp_repr*/
    0,                               /*tp_as_number*/
    0,                               /*tp_as_sequence*/
    0,                               /*tp_as_mapping*/
    0,                               /*tp_hash */
    0,                               /*tp_call*/
    0,                               /*tp_str*/
    0,                               /*tp_getattro*/
    0,                               /*tp_setattro*/
    0,                               /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    "Async objects",                 /* tp_doc */
    0,                               /* tp_traverse */
    0,                               /* tp_clear */
    0,                               /* tp_richcompare */
    0,                               /* tp_weaklistoffset */
    0,                               /* tp_iter */
    0,                               /* tp_iternext */
    Async_methods,                   /* tp_methods */
    0,                               /* tp_members */
    0,                               /* tp_getset */
    0,                               /* tp_base */
    0,                               /* tp_dict */
    0,                               /* tp_descr_get */
    0,                               /* tp_descr_set */
    0,                               /* tp_dictoffset */
    (initproc)Async_init,            /* tp_init */
};

typedef struct libevwrapper_Prepare {
    PyObject_HEAD
    struct ev_prepare prepare;
    struct libevwrapper_Loop *loop;
    PyObject *callback;
} libevwrapper_Prepare;

static void
Prepare_dealloc(libevwrapper_Prepare *self) {
    Py_XDECREF(self->loop);
    Py_XDECREF(self->callback);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static void prepare_callback(struct ev_loop *loop, ev_prepare *watcher, int revents) {
    if (!Py_IsInitialized()) {
        return;
    }

    libevwrapper_Prepare *self = watcher->data;
    PyObject *result = NULL;
    PyGILState_STATE gstate;

    gstate = PyGILState_Ensure();
    result = PyObject_CallFunction(self->callback, "O", self);
    if (!result) {
        PyErr_WriteUnraisable(self->callback);
    }
    Py_XDECREF(result);

    PyGILState_Release(gstate);
}

static int
Prepare_init(libevwrapper_Prepare *self, PyObject *args, PyObject *kwds) {
    PyObject *callback;
    PyObject *loop;
    struct ev_prepare *prepare = NULL;

    if (!PyArg_ParseTuple(args, "OO", &loop, &callback)) {
        return -1;
    }

    if (loop) {
        Py_INCREF(loop);
        self->loop = (libevwrapper_Loop *)loop;
    } else {
        return -1;
    }

    if (callback) {
        if (!PyCallable_Check(callback)) {
            PyErr_SetString(PyExc_TypeError, "callback parameter must be callable");
            Py_XDECREF(loop);
            return -1;
        }
        Py_INCREF(callback);
        self->callback = callback;
    }
    prepare = &(self->prepare);
    ev_prepare_init(prepare, prepare_callback);
    self->prepare.data = self;
    return 0;
}

static PyObject *
Prepare_start(libevwrapper_Prepare *self, PyObject *args) {
    ev_prepare_start(self->loop->loop, &self->prepare);
    Py_RETURN_NONE;
}

static PyObject *
Prepare_stop(libevwrapper_Prepare *self, PyObject *args) {
    ev_prepare_stop(self->loop->loop, &self->prepare);
    Py_RETURN_NONE;
}

static PyMethodDef Prepare_methods[] = {
    {"start", (PyCFunction)Prepare_start, METH_NOARGS, "Start the Prepare watcher"},
    {"stop", (PyCFunction)Prepare_stop, METH_NOARGS, "Stop the Prepare watcher"},
    {NULL}  /* Sentinal */
};

static PyTypeObject libevwrapper_PrepareType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "cassandra.io.libevwrapper.Prepare",  /*tp_name*/
    sizeof(libevwrapper_Prepare),    /*tp_basicsize*/
    0,                               /*tp_itemsize*/
    (destructor)Prepare_dealloc,     /*tp_dealloc*/
    0,                               /*tp_print*/
    0,                               /*tp_getattr*/
    0,                               /*tp_setattr*/
    0,                               /*tp_compare*/
    0,                               /*tp_repr*/
    0,                               /*tp_as_number*/
    0,                               /*tp_as_sequence*/
    0,                               /*tp_as_mapping*/
    0,                               /*tp_hash */
    0,                               /*tp_call*/
    0,                               /*tp_str*/
    0,                               /*tp_getattro*/
    0,                               /*tp_setattro*/
    0,                               /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    "Prepare objects",               /* tp_doc */
    0,                               /* tp_traverse */
    0,                               /* tp_clear */
    0,                               /* tp_richcompare */
    0,                               /* tp_weaklistoffset */
    0,                               /* tp_iter */
    0,                               /* tp_iternext */
    Prepare_methods,                 /* tp_methods */
    0,                               /* tp_members */
    0,                               /* tp_getset */
    0,                               /* tp_base */
    0,                               /* tp_dict */
    0,                               /* tp_descr_get */
    0,                               /* tp_descr_set */
    0,                               /* tp_dictoffset */
    (initproc)Prepare_init,          /* tp_init */
};

typedef struct libevwrapper_Timer {
    PyObject_HEAD
    struct ev_timer timer;
    struct libevwrapper_Loop *loop;
    PyObject *callback;
} libevwrapper_Timer;

static void
Timer_dealloc(libevwrapper_Timer *self) {
    Py_XDECREF(self->loop);
    Py_XDECREF(self->callback);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static void timer_callback(struct ev_loop *loop, ev_timer *watcher, int revents) {
    if (!Py_IsInitialized()) {
        return;
    }

    libevwrapper_Timer *self = watcher->data;

    PyObject *result = NULL;
    PyGILState_STATE gstate;

    gstate = PyGILState_Ensure();
    result = PyObject_CallFunction(self->callback, NULL);
    if (!result) {
        PyErr_WriteUnraisable(self->callback);
    }
    Py_XDECREF(result);

    PyGILState_Release(gstate);
}

static int
Timer_init(libevwrapper_Timer *self, PyObject *args, PyObject *kwds) {
    PyObject *callback;
    PyObject *loop;

    if (!PyArg_ParseTuple(args, "OO", &loop, &callback)) {
        return -1;
    }

    if (loop) {
        Py_INCREF(loop);
        self->loop = (libevwrapper_Loop *)loop;
    } else {
        return -1;
    }

    if (callback) {
        if (!PyCallable_Check(callback)) {
            PyErr_SetString(PyExc_TypeError, "callback parameter must be callable");
            Py_XDECREF(loop);
            return -1;
        }
        Py_INCREF(callback);
        self->callback = callback;
    }
    ev_init(&self->timer, timer_callback);
    self->timer.data = self;
    return 0;
}

static PyObject *
Timer_start(libevwrapper_Timer *self, PyObject *args) {
    double timeout;
    if (!PyArg_ParseTuple(args, "d", &timeout)) {
        return NULL;
    }
    /* some tiny non-zero number to avoid zero, and
     make it run immediately for negative timeouts */
    self->timer.repeat = fmax(timeout, 0.000000001);
    ev_timer_again(self->loop->loop, &self->timer);
    Py_RETURN_NONE;
}

static PyObject *
Timer_stop(libevwrapper_Timer *self, PyObject *args) {
    ev_timer_stop(self->loop->loop, &self->timer);
    Py_RETURN_NONE;
}

static PyMethodDef Timer_methods[] = {
    {"start", (PyCFunction)Timer_start, METH_VARARGS, "Start the Timer watcher"},
    {"stop", (PyCFunction)Timer_stop, METH_NOARGS, "Stop the Timer watcher"},
    {NULL}  /* Sentinal */
};

static PyTypeObject libevwrapper_TimerType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "cassandra.io.libevwrapper.Timer",  /*tp_name*/
    sizeof(libevwrapper_Timer),      /*tp_basicsize*/
    0,                               /*tp_itemsize*/
    (destructor)Timer_dealloc,       /*tp_dealloc*/
    0,                               /*tp_print*/
    0,                               /*tp_getattr*/
    0,                               /*tp_setattr*/
    0,                               /*tp_compare*/
    0,                               /*tp_repr*/
    0,                               /*tp_as_number*/
    0,                               /*tp_as_sequence*/
    0,                               /*tp_as_mapping*/
    0,                               /*tp_hash */
    0,                               /*tp_call*/
    0,                               /*tp_str*/
    0,                               /*tp_getattro*/
    0,                               /*tp_setattro*/
    0,                               /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    "Timer objects",                 /* tp_doc */
    0,                               /* tp_traverse */
    0,                               /* tp_clear */
    0,                               /* tp_richcompare */
    0,                               /* tp_weaklistoffset */
    0,                               /* tp_iter */
    0,                               /* tp_iternext */
    Timer_methods,                   /* tp_methods */
    0,                               /* tp_members */
    0,                               /* tp_getset */
    0,                               /* tp_base */
    0,                               /* tp_dict */
    0,                               /* tp_descr_get */
    0,                               /* tp_descr_set */
    0,                               /* tp_dictoffset */
    (initproc)Timer_init,            /* tp_init */
};


static PyMethodDef module_methods[] = {
    {NULL}  /* Sentinal */
};

PyDoc_STRVAR(module_doc,
"libev wrapper methods");

static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "libevwrapper",
    module_doc,
    -1,
    module_methods,
    NULL,
    NULL,
    NULL,
    NULL
};

#define INITERROR return NULL

PyObject *
PyInit_libevwrapper(void)
{
    PyObject *module = NULL;

    if (PyType_Ready(&libevwrapper_LoopType) < 0)
        INITERROR;

    libevwrapper_IOType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&libevwrapper_IOType) < 0)
        INITERROR;

    libevwrapper_PrepareType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&libevwrapper_PrepareType) < 0)
        INITERROR;

    libevwrapper_AsyncType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&libevwrapper_AsyncType) < 0)
        INITERROR;

    libevwrapper_TimerType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&libevwrapper_TimerType) < 0)
        INITERROR;

    module = PyModule_Create(&moduledef);

    if (module == NULL)
        INITERROR;

    if (PyModule_AddIntConstant(module, "EV_READ", EV_READ) == -1)
        INITERROR;
    if (PyModule_AddIntConstant(module, "EV_WRITE", EV_WRITE) == -1)
        INITERROR;
    if (PyModule_AddIntConstant(module, "EV_ERROR", EV_ERROR) == -1)
        INITERROR;

    Py_INCREF(&libevwrapper_LoopType);
    if (PyModule_AddObject(module, "Loop", (PyObject *)&libevwrapper_LoopType) == -1)
        INITERROR;

    Py_INCREF(&libevwrapper_IOType);
    if (PyModule_AddObject(module, "IO", (PyObject *)&libevwrapper_IOType) == -1)
        INITERROR;

    Py_INCREF(&libevwrapper_PrepareType);
    if (PyModule_AddObject(module, "Prepare", (PyObject *)&libevwrapper_PrepareType) == -1)
        INITERROR;

    Py_INCREF(&libevwrapper_AsyncType);
    if (PyModule_AddObject(module, "Async", (PyObject *)&libevwrapper_AsyncType) == -1)
        INITERROR;

    Py_INCREF(&libevwrapper_TimerType);
    if (PyModule_AddObject(module, "Timer", (PyObject *)&libevwrapper_TimerType) == -1)
        INITERROR;

#if PY_MAJOR_VERSION < 3 || (PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION < 7)
    // Since CPython 3.7, `Py_Initialize()` routing always initializes GIL.
    // Routine `PyEval_ThreadsInitialized()` has been deprecated in CPython 3.7
    // and completely removed in CPython 3.13.
    if (!PyEval_ThreadsInitialized()) {
        PyEval_InitThreads();
    }
#endif

    return module;
}
