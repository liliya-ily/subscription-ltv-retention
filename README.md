<h1 align="center">Subscription LTV & Retention</h1>

<p align="center">
  Анализ жизненного цикла подписчиков музыкального стримингового сервиса: LTV, отток и удержание.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/DuckDB-SQL-FFF000?logo=duckdb&logoColor=black" />
  <img src="https://img.shields.io/badge/Docker-ready-2496ED?logo=docker&logoColor=white" />
  <img src="https://img.shields.io/badge/MLflow-3.15.1-0194E2?logo=mlflow&logoColor=white" />
</p>

## О проекте

Проект на публичных данных подписочного сервиса: как ведут себя клиенты после оформления подписки, сколько денег они приносят и кого стоит удерживать.

## Данные

Соревнование Kaggle [WSDM – KKBox's Churn Prediction Challenge](https://www.kaggle.com/c/kkbox-churn-prediction-challenge): транзакции, логи прослушивания и профили пользователей за 2015–2017 годы.

Данные не входят в репозиторий и распространяются на условиях правил соревнования. Для воспроизведения их нужно скачать самостоятельно и положить в `data/`.

## Стек

Python 3.12, DuckDB, pandas, scikit-learn, MLflow, Docker, VS Code Dev Containers.

## Запуск

Окружение поднимается через Dev Containers, подробности в [setup.md](setup.md).
