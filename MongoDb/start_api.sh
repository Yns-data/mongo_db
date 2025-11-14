#!/usr/bin/env bash



./venv/bin/python ./venv/bin/uvicorn mongo_db_interaction.api:app --reload
