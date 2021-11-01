FROM continuumio/miniconda3 AS build

COPY . /laserfarm

WORKDIR /laserfarm
RUN apt-get update --allow-releaseinfo-change && apt-get -y install gcc g++
RUN conda env create -f environment.yml
RUN conda install -c conda-forge conda-pack
RUN conda-pack -n laserfarm -o /tmp/env.tar && \
    mkdir /venv && cd /venv && tar xf /tmp/env.tar && \
    rm /tmp/env.tar
RUN /venv/bin/conda-unpack

FROM debian:buster AS runtime
COPY --from=build /venv /venv
ENV PATH=/venv/bin:${PATH}