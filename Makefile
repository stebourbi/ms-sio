SHELL := /bin/bash
BASE_DIR := $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))
DATASETS_PATH=~/cours-hdp2/datasets


prepare-dataset:
	@mkdir -p $(DATASETS_PATH)/raw
	@unzip -o ~/Downloads/youtube-new.zip -d /tmp
	@mv /tmp/FRvideos.csv $(DATASETS_PATH)/raw/FRvideos.csv
	@mv /tmp/FR_category_id.json $(DATASETS_PATH)/raw/FR_category_id.json



run-pyspark:
	@docker run --rm -ti -v $(DATASETS_PATH):/data -p 4040:4040 stebourbi/sio:pyspark


open-spark-ui:
	@open http://localhost:4040


prepare-dev-env:
	@source $(BASE_DIR)/create-virtual-dev-env.sh

