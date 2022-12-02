use std::collections::{HashMap, HashSet};

use anyhow::Result;

use pyo3::exceptions::PyValueError;
use pyo3::types::{PyTuple, PyBool};
use pyo3::{Py, PyAny, IntoPy, PyObject, intern, FromPyObject, Python};

#[derive(Hash, PartialEq, Eq, Clone, Copy)]
pub enum Role {
    Search,
    Worker
}

impl IntoPy<PyObject> for Role {
    fn into_py(self, py: pyo3::Python<'_>) -> PyObject {
        match self {
            Role::Search => intern!(py, "Search").into(),
            Role::Worker => intern!(py, "Worker").into(),
        }
    }
}

impl ToString for Role {
    fn to_string(&self) -> String {
        match self {
            Role::Search => "Search".to_owned(),
            Role::Worker => "Worker".to_owned(),
        }
    }
}

impl<'source> FromPyObject<'source> for Role {
    fn extract(ob: &'source PyAny) -> pyo3::PyResult<Self> {
        let value: String = ob.extract()?;
        let value = value.to_lowercase();
        if value == "search" {
            Ok(Role::Search)
        } else if value == "worker" {
            Ok(Role::Worker)
        } else {
            Err(PyValueError::new_err(format!("Not accepted role catagory: {value}")))
        }
    }
}


pub enum Authenticator {
    Static(HashMap<String, HashSet<Role>>),
    Python(PythonAuthenticator)
}

impl Authenticator {

    pub fn new_static(assignments: HashMap<String, HashSet<Role>>) -> Result<Self> {
        // let mut assign: HashMap<String, HashSet<Role>> = Default::default();
        // for (key, role_names) in assignments.into_iter() {
        //     let mut roles = HashSet::new();
        //     for name in role_names {
        //         roles.insert(name.try_into()?);
        //     }
        //     assign.insert(key, roles);
        // }
        Ok(Authenticator::Static(assignments))
    }

    pub fn new_python(object: Py<PyAny>) -> Result<Self> {
        Ok(Authenticator::Python(PythonAuthenticator::new(object)))
    }

    pub fn get_roles(&self, token: &str) -> Result<HashSet<Role>> {
        match self {
            Authenticator::Static(data) => Ok(match data.get(token) {
                Some(roles) => roles.clone(),
                None => Default::default()
            }),
            Authenticator::Python(obj) => obj.get_roles(token),
        }
    }

    pub fn is_role_assigned(&self, token: &str, role: Role) -> bool {
        match self {
            Authenticator::Static(data) => match data.get(token) {
                Some(roles) => roles.contains(&role),
                None => false,
            },
            Authenticator::Python(obj) => obj.is_role_assigned(token, role),
        }
    }
}


#[derive(Clone)]
pub struct PythonAuthenticator {
    object: Py<PyAny>
}

impl PythonAuthenticator {
    pub fn new(object: Py<PyAny>) -> Self {
        Self {object}
    }

    pub fn get_roles(&self, token: &str) -> Result<HashSet<Role>> {
        // Invoke method
        Ok(Python::with_gil(|py| {
            // calling the py_sleep method like a normal function returns a coroutine
            let args = PyTuple::new(py, &[token.to_string()]);
            let result = self.object.call_method1(py, "get_roles", args)?;
            let result: HashSet<Role> = result.extract(py).unwrap_or_default();
            anyhow::Ok(result)
        })?)
    }

    pub fn is_role_assigned(&self, token: &str, role: Role) -> bool {
        // Invoke method
        Python::with_gil(|py| {
            // calling the py_sleep method like a normal function returns a coroutine
            let args = PyTuple::new(py, &[token.to_string(), role.to_string()]);
            let result = self.object.call_method1(py, "is_role_assigned", args).unwrap_or(PyBool::new(py, false).into());
            let result: bool = result.extract(py).unwrap_or(false);
            result
        })
    }
}