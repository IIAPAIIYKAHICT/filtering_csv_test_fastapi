FROM python:3.12

ENV PYTHONUNBUFFERED=1

ARG WORKDIR=/wd
ARG USER=user

WORKDIR ${WORKDIR}

RUN useradd --system ${USER} && \
    chown --recursive ${USER} ${WORKDIR}

## Note: Use it, if you use volumes in "docker space", not "bind mounts".
## Create a directories: "logs", "files_output", "files_input". Change the owner to ${USER}
#RUN mkdir logs files_output files_input && \
#    chown --recursive ${USER} logs files_output files_input

RUN apt update && apt upgrade --yes

COPY --chown=${USER} requirements.txt requirements.txt

RUN pip install --upgrade pip && \
    pip install --requirement requirements.txt

COPY --chown=${USER} main.py main.py
COPY --chown=${USER} ./dataset dataset
COPY --chown=${USER} ./templates templates
COPY --chown=${USER} ./model model
COPY --chown=${USER} ./parsing_scripts parsing_scripts
COPY --chown=${USER} ./utils utils
COPY --chown=${USER} ./static static

USER ${USER}

ENTRYPOINT ["uvicorn", "main:app"]

CMD ["--help"]
