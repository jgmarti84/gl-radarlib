# Plan de despliegue en producción
## Cambios radarlib + webmet25 — Agosto 2026

**Preparado por:** Javier Marti  
**Fecha:** 28 de agosto de 2026

---

## Resumen ejecutivo

Los cambios de este ciclo se distribuyen en dos repositorios: **radarlib** (pipeline de generación de productos radar) y **webmet25** (API, indexer y frontend web). Existe una dependencia de dirección entre ambos que determina el orden obligatorio de despliegue.

**Regla general:** desplegar **webmet25 primero**, radarlib después.

---

## 1. Por qué el orden importa

### 1.1 El cambio central en radarlib

La refactorización de timestamps (`Group B`) elimina la escritura doble de archivos COG y GeoJSON con timestamps redondeados. A partir de este cambio, cada archivo lleva el `obs_dt` exacto del volumen BUFR en su nombre:

```
Antes:  RMA5_0315_01_20260823T200000Z_COLMAX.tif   (timestamp redondeado)
Ahora:  RMA5_0315_01_20260823T195438Z_COLMAX.tif   (timestamp exacto)
```

El indexer de webmet25 registra en la base de datos la `observation_time` tal como figura en el nombre del archivo.

### 1.2 Qué ocurre si radarlib se despliega primero

El frontend anterior al ciclo actual agrupa los COGs usando `groupCogsByTimestamp()` con una ventana de **5 minutos** (bucketing a la frontera de 5 min más cercana):

- Un COG antiguo con `obs_time = 20:00:00` → slot `20:00`
- Un COG nuevo con `obs_time = 19:54:38` → slot `19:55`

Los COGs no desaparecen, pero cada radar termina en un slot diferente. Durante el despliegue escalonado de radarlib —un servicio `genpro25` por radar— habrá radares produciendo timestamps redondeados y otros produciendo timestamps exactos. El resultado es que el frontend no puede agrupar todos los radares en el mismo frame de animación: **la sincronización multi-radar se rompe**. El mapa muestra cada radar en su propio instante, como si no tuvieran datos contemporáneos entre sí.

### 1.3 Qué ocurre si webmet25 se despliega primero

El nuevo frontend usa `buildGridFrames()` con una grilla de **10 minutos** y `Math.round`:

- COG antiguo `20:00:00` → `Math.round(20:00 / 10min)` → slot `20:00` ✓
- COG nuevo `19:54:38` → `Math.round(19:54:38 / 10min)` → slot `19:50` ✓

Ambos tipos de timestamp se mapean correctamente a la grilla de 10 minutos. La animación permanece coherente tanto con los datos producidos por la versión antigua de radarlib como con los de la nueva. **El despliegue de webmet25 es compatible en ambas direcciones.**

---

## 2. Orden de despliegue recomendado

```
PASO 1 — webmet25 (backend + indexer + frontend)
PASO 2 — radarlib en cada servicio genpro25
PASO 3 — activación de COLMAX/TOPS_AND_CORES en ARX (opcional, post-verificación)
```

---

## 3. Detalle de cada paso

### PASO 1 — Despliegue de webmet25

Componentes a actualizar:

| Componente | Cambio | Método |
|---|---|---|
| API (FastAPI) | Cache busting por mtime, TTL de metadatos 300→45 s, ventana reciente 10→20 min | Restart del contenedor `api` |
| Indexer | Ya acepta timestamps exactos (`\d{8}T\d{6}Z`). Sin cambio de esquema DB. | Restart del contenedor `indexer` |
| Frontend (`frontend-v2`) | Nuevo `buildGridFrames`, grilla anclada, hold-last-frame, topes/núcleos precargados, lock-to-latest | **Rebuild** de imagen + restart |

**Verificación post-paso 1** (con radarlib aún sin actualizar):
- La animación debe seguir mostrando todos los radares sincronizados.
- Los topes y núcleos deben cargar al iniciar la animación sin demora por frame.
- El botón "último frame" debe retener el frame final en lugar de rebobinar.
- Al seleccionar un radar con un slot sin COG, el frame anterior de ese radar debe permanecer visible (hold-last-frame).

---

### PASO 2 — Despliegue de radarlib en los servicios genpro25

Cada radar tiene su propio servicio `genpro25`. Actualizar e instalar la nueva versión de radarlib en cada uno y reiniciar el servicio.

**Efecto en producción durante el despliegue escalonado:**

- Los primeros radares actualizados empezarán a producir COGs con timestamps exactos.
- El frontend (ya actualizado en el paso 1) los agrupará correctamente en la grilla de 10 minutos junto con los COGs de timestamp redondeado de los radares aún no actualizados.
- No se produce ninguna rotura de animación.

**Datos históricos:**

- Los COGs anteriores ya indexados en la base de datos mantienen sus timestamps redondeados. El frontend los gestiona sin modificación.
- Los servicios con radarlib antiguo que generaban dos archivos por observación (ceiled + rounded) pueden dejar duplicados en la base de datos para observaciones pasadas. Estos duplicados aparecen en dos slots adyacentes y no causan errores, pero pueden verse como dos frames del mismo radar con datos idénticos. Se corrigen solos a medida que envejecen fuera de la ventana de animación.

**Verificación post-paso 2:**
- Confirmar que los nuevos COGs aparecen con timestamps exactos en los logs del indexer.
- Verificar en la animación que los radares actualizados se sincronizan correctamente con los demás.

---

### PASO 3 — Activación de COLMAX y TOPS_AND_CORES en radares ARX (condicional)

Este paso aplica solo a los radares ARX y requiere verificación previa:

1. Confirmar que el volumen 03 de los ARX incluye el campo RHOHV (necesario para el filtro de calidad en la detección de núcleos).
2. Una vez confirmado, actualizar la configuración `genpro25_arX.yml` con los nuevos tipos de volumen (01 DBZH, 02 VRAD, 03 DBZH+COLMAX+TOPS\_AND\_CORES).
3. Reiniciar los servicios `genpro25` correspondientes a los ARX.

Los radares ARX comenzarán a producir productos COLMAX y GeoJSON de topes/núcleos. Estos aparecerán en el frontend automáticamente a través de los modos de cobertura multi-cláusula (que ya están incluidos en el paso 1).

**Este paso NO debe realizarse antes del paso 1**, ya que el soporte de cobertura multi-cláusula (AR strategy 1000 + RMA strategy 0315) forma parte de los cambios de webmet25.

---

## 4. Resumen de compatibilidades

| Combinación | Resultado |
|---|---|
| webmet25 nuevo + radarlib antiguo | ✅ Compatible. Animación coherente con timestamps redondeados. |
| webmet25 nuevo + radarlib nuevo | ✅ Compatible. Animación coherente con timestamps exactos. |
| webmet25 antiguo + radarlib nuevo | ⚠️ Animación pierde sincronización multi-radar durante la transición escalonada. |
| webmet25 antiguo + radarlib antiguo | ✅ Estado anterior a estos cambios, sin modificaciones. |

---

## 5. Rollback

En caso de necesitar revertir:

- **webmet25:** redeployar la imagen anterior del frontend y reiniciar la API. Los datos en base de datos no se ven afectados (sin migraciones de esquema en este ciclo).
- **radarlib:** reinstalar la versión anterior en los servicios `genpro25` y reiniciarlos. Los COGs ya generados con timestamp exacto permanecen en disco e indexados; son plenamente compatibles con el frontend anterior (aparecen en slots de 5 minutos en lugar de 10, sin pérdida de datos).
- El paso 3 (ARX) puede revertirse desactivando `ADD_COLMAX` y `ADD_TOPS_AND_CORES` en la configuración y reiniciando el servicio, sin efecto en datos ya producidos.
