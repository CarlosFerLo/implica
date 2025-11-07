---
noteId: "f40a26a0bbeb11f0a4b86baaf96c2d6a"
tags: []
---

# Contributing to Implica

¡Gracias por tu interés en contribuir a Implica! Esta guía te ayudará a configurar el entorno de desarrollo y entender la estructura del proyecto.

## Requisitos Previos

- **Rust** 1.70+ (instalado vía [rustup](https://rustup.rs/))
- **Python** 3.8+ (se recomienda 3.12)
- **Maturin** 1.9+ (`pip install maturin`)

## Configuración del Entorno

```bash
# Clonar el repositorio
git clone <repository-url>
cd implica

# Instalar dependencias de desarrollo
pip install -e ".[dev]"

# Configurar Git hooks (recomendado)
./setup-hooks.sh

# Construir e instalar en modo desarrollo
maturin develop

# Verificar la instalación
python -c "import implica; print('✓ Implica instalado correctamente')"
```

### Git Hooks

El proyecto incluye hooks de Git que automáticamente:

- Formatean código Rust con `cargo fmt`
- Verifican código Rust con `cargo clippy`
- Formatean código Python con `black`
- Previenen commits si hay errores de linting que no pueden auto-corregirse

**Configuración**: Ejecuta `./setup-hooks.sh` después de clonar el repositorio.

**Bypass temporal** (no recomendado): `git commit --no-verify`

## Estructura del Proyecto

```
implica/
├── src/                    # Código fuente Rust
│   ├── lib.rs             # Módulo PyO3 principal
│   ├── types.rs           # Sistema de tipos (Variable, Application)
│   ├── term.rs            # Términos con aplicación
│   ├── graph.rs           # Grafo (Node, Edge, Graph)
│   ├── type_schema.rs     # Patrones de tipos
│   ├── patterns.rs        # Patrones de consulta
│   └── query.rs           # Sistema de consultas
├── API.pyi                # Especificación de la API Python
├── test_api.py            # Suite de pruebas
├── examples.py            # Ejemplos de uso
├── README.md              # Documentación principal
├── TESTING.md             # Guía de pruebas
└── Cargo.toml             # Configuración Rust

```

## Flujo de Desarrollo

### 1. Hacer Cambios en el Código Rust

```bash
# Editar archivos en src/
vim src/types.rs

# Reconstruir
maturin develop

# Probar
python test_api.py
```

### 2. Agregar Nuevas Funcionalidades

#### Agregar un nuevo tipo o clase:

1. **Crear el struct en Rust** con decoradores PyO3:

   ```rust
   #[pyclass]
   pub struct MiNuevoTipo {
       #[pyo3(get)]
       pub nombre: String,
   }

   #[pymethods]
   impl MiNuevoTipo {
       #[new]
       fn new(nombre: String) -> Self {
           Self { nombre }
       }
   }
   ```

2. **Registrar en `lib.rs`**:

   ```rust
   #[pymodule]
   fn implica(m: &Bound<'_, PyModule>) -> PyResult<()> {
       m.add_class::<MiNuevoTipo>()?;
       // ... otros tipos
       Ok(())
   }
   ```

3. **Actualizar `API.pyi`**:

   ```python
   class MiNuevoTipo:
       nombre: str
       def __init__(self, nombre: str) -> None: ...
   ```

4. **Agregar pruebas en `test_api.py`**:
   ```python
   def test_mi_nuevo_tipo():
       obj = implica.MiNuevoTipo("test")
       assert obj.nombre == "test"
   ```

### 3. Trabajar con Tipos PyO3

#### Tipos no clonables (Py<PyDict>, Py<PyAny>):

```rust
use pyo3::prelude::*;

#[pyclass]
struct MiClase {
    data: Py<PyDict>,  // No implementa Clone
}

// Implementar Clone manualmente
impl Clone for MiClase {
    fn clone(&self) -> Self {
        Python::with_gil(|py| {
            Self {
                data: self.data.clone_ref(py),
            }
        })
    }
}
```

#### Conversión Rust ↔ Python:

```rust
// Rust -> Python
fn to_python(obj: &MiStruct, py: Python) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("field", &obj.field)?;
    Ok(dict.into())
}

// Python -> Rust
fn from_python(obj: &Bound<'_, PyAny>) -> PyResult<MiStruct> {
    let field: String = obj.extract()?;
    Ok(MiStruct { field })
}
```

### 4. Patrones Comunes

#### Getters para Arc<T>:

```rust
#[pyclass]
struct Wrapper {
    inner: Arc<Data>,  // No usar #[pyo3(get)]
}

#[pymethods]
impl Wrapper {
    #[getter]
    fn inner(&self, py: Python) -> PyResult<PyObject> {
        data_to_python(&self.inner, py)
    }
}
```

#### Métodos mutables:

```rust
#[pymethods]
impl Graph {
    fn add_node(&mut self, node: Node) {
        self.nodes.push(node);
    }
}
```

## Guías de Estilo

### Rust

- Usa `rustfmt` antes de hacer commit: `cargo fmt`
- Sigue las convenciones de Rust (snake_case para funciones, CamelCase para tipos)
- Documenta funciones públicas con `///`
- Agrega tests unitarios en módulos con `#[cfg(test)]`

### Python

- Sigue PEP 8
- Usa type hints en `API.pyi`
- Documenta funciones con docstrings

## Pruebas

```bash
# Pruebas Python (principal)
python test_api.py
python examples.py

# Verificar tipos
mypy API.pyi

# Formatear código Rust
cargo fmt

# Linter Rust
cargo clippy
```

## Debugging

### Errores de compilación Rust:

```bash
# Ver errores detallados
maturin develop --release 2>&1 | less

# Verificar sintaxis sin construir
cargo check
```

### Errores de runtime Python:

```python
import traceback
try:
    # tu código
except Exception as e:
    traceback.print_exc()
```

### Imprimir desde Rust:

```rust
use pyo3::prelude::*;

#[pymethods]
impl MiClase {
    fn debug(&self, py: Python) {
        py.run(
            &format!("print('Debug: {}')", self.valor),
            None,
            None,
        ).unwrap();
    }
}
```

## Problemas Comunes

### Error: "symbol(s) not found" con cargo test

**Causa**: El proyecto es `cdylib`, no puede ejecutarse standalone.

**Solución**: Usa `python test_api.py` en su lugar (ver `TESTING.md`).

### Error: "IntoPyObject not implemented for Arc<T>"

**Causa**: PyO3 no puede convertir Arc automáticamente.

**Solución**: Crea un getter manual:

```rust
#[getter]
fn field(&self, py: Python) -> PyResult<PyObject> {
    convert_to_python(&self.field, py)
}
```

### Error: "Clone not implemented for Py<PyDict>"

**Causa**: Los tipos Py<T> no implementan Clone.

**Solución**: Implementa Clone manualmente con `clone_ref(py)`.

## Submitting Changes

1. **Fork** el repositorio
2. **Crea una branch**: `git checkout -b feature/mi-feature`
3. **Haz commits** con mensajes descriptivos
4. **Prueba todo**: `maturin develop && python test_api.py`
5. **Formatea**: `cargo fmt`
6. **Push**: `git push origin feature/mi-feature`
7. **Abre un Pull Request**

## Arquitectura

### Capas del Sistema

```
┌─────────────────────────────────────┐
│         Python API (API.pyi)         │
├─────────────────────────────────────┤
│      PyO3 Bindings (lib.rs)          │
├─────────────────────────────────────┤
│  Query System (query.rs, patterns.rs)│
├─────────────────────────────────────┤
│   Graph Model (graph.rs)             │
├─────────────────────────────────────┤
│   Type System (types.rs, term.rs)    │
└─────────────────────────────────────┘
```

### Flujo de Datos

1. **Python** → PyO3 convierte argumentos
2. **Rust** procesa la lógica
3. **PyO3** convierte resultados → Python

### Dependencias entre Módulos

```
lib.rs
  ├─→ types.rs (base: Variable, Application)
  ├─→ term.rs (usa types)
  ├─→ graph.rs (usa types, term)
  ├─→ type_schema.rs (usa types)
  ├─→ patterns.rs (usa type_schema, graph)
  └─→ query.rs (usa patterns, graph)
```

## Recursos

- [PyO3 Documentation](https://pyo3.rs/)
- [Maturin Guide](https://www.maturin.rs/)
- [Rust Book](https://doc.rust-lang.org/book/)
- [API Specification](API.pyi)

## Licencia

[Tu licencia aquí]

## Contacto

[Tu información de contacto]
