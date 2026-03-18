# Variáveis
IMAGE_NAME = olist-spark-image
DATA_VOL = "$$(pwd)/data:/app/data"
SRC_VOL = "$$(pwd)/src:/app/src"

.PHONY: build run-bronze run-silver run-gold audit

# Constrói a imagem do Docker
build:
	docker build -t $(IMAGE_NAME) .

# Executa a ingestão para a camada Bronze
run-bronze:
	docker run --rm -v $(DATA_VOL) -v $(SRC_VOL) $(IMAGE_NAME) python src/jobs/ingest_bronze.py

# Processa os dados para a camada Silver
run-silver:
	docker run --rm -v $(DATA_VOL) -v $(SRC_VOL) $(IMAGE_NAME) python src/jobs/silver_process.py

# Modela os dados para a camada Gold
run-gold:
	docker run --rm -v $(DATA_VOL) -v $(SRC_VOL) $(IMAGE_NAME) python src/jobs/gold_process.py

# Roda a auditoria visual da Silver
audit:
	docker run --rm -v $(DATA_VOL) -v $(SRC_VOL) $(IMAGE_NAME) python src/jobs/silver_audit.py