# Quick Start

Шаблон для локальной разработки DS/ML-проектов в Docker.

Внутри:

- Python 3.12
- Jupyter
- pandas / numpy / scikit-learn / matplotlib
- MLflow
- VS Code Dev Containers

## Запуск

Нужно установить:

- WSL 2
- Ubuntu
- Docker Desktop
- VS Code
- расширение **Dev Containers**

Клонировать репозиторий:

```bash
git clone <repository-url>
cd <project>
code .
```

В VS Code:

```text
Ctrl + Shift + P
→ Dev Containers: Reopen in Container
```

После запуска проект будет открыт внутри контейнера в:

```text
/workspace
```

Проверить Python:

```bash
python --version
```

В notebook:

```python
import sys
print(sys.executable)
```

Ожидаемый путь:

```text
/usr/local/bin/python
```

## Dev Containers

`.devcontainer/devcontainer.json` нужен, чтобы VS Code сам подключался к нужному Docker-контейнеру.

Без него пришлось бы отдельно запускать контейнер, подключаться через `Attach to Running Container` и выбирать окружение для Jupyter.

Здесь достаточно открыть проект через:

```text
Dev Containers: Reopen in Container
```

## MLflow

MLflow запускается отдельным сервисом через Docker Compose.

UI:

```text
http://localhost:5000
```

Из notebook test.ipynb:

```python
import mlflow

mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("my_experiment")
```

Пример run:

```python
with mlflow.start_run():
    mlflow.log_param("model", "baseline")
    mlflow.log_metric("rmse", 123.45)
```

Внутри Docker используется адрес:

```text
http://mlflow:5000
```

Из Windows:

```text
http://localhost:5000
```

## Файлы проекта

В `docker-compose.yml` папка проекта монтируется в `/workspace`:

```yaml
volumes:
  - .:/workspace
```

Поэтому файлы в Windows и `/workspace` внутри контейнера — это одни и те же файлы.

Docker здесь используется только как окружение. Код, notebooks и конфигурация остаются обычными файлами проекта и могут храниться в Git.

## Остановка

```bash
docker compose down
```

При обычной работе через Dev Containers вручную запускать `docker compose up` не требуется.

## Возможные проблемы

### `docker: command not found`

Если Docker был установлен после запуска VS Code, полностью перезапустить VS Code.

Проверка:

```bash
docker --version
```

### Docker установлен, но контейнеры не запускаются

Если появляется ошибка подключения к Docker API, проверить, что Docker Desktop запущен.

### Ошибка `ubuntu.sock` при запуске Dev Container

Проверить:

```powershell
wsl -l -v
```

Ubuntu должна работать через WSL 2.

В Docker Desktop открыть:

```text
Settings → Resources → WSL Integration
```

и включить интеграцию с Ubuntu.

### MLflow: `403 Invalid Host header`

Для доступа из соседнего Docker-контейнера MLflow должен разрешать hostname `mlflow:5000`.

В команде запуска сервера:

```text
--allowed-hosts "mlflow:5000,localhost:5000,127.0.0.1:5000"
```

### Jupyter использует локальный Python

Проверить:

```python
import sys
print(sys.executable)
```

В контейнере должно быть:

```text
/usr/local/bin/python
```

а не путь вида:

```text
C:\Users\...
```