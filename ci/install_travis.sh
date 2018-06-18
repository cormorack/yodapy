#!/bin/bash

echo "[install_travis]"

home_dir=$(pwd)
echo
echo "[home_dir]: $home_dir"

MINICONDA_DIR="$HOME/miniconda"

URL=""
if ["$TRAVIS_OS_NAME" == "osx"] ; then
  URL="http://repo.continuum.io/miniconda/Miniconda3-latest-MacOSX-x86_64.sh"
  TRAVIS_PYTHON_VERSION=3.6
else
  URL="http://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh"
fi

if [ ! -f $MINICONDA_DIR/bin/conda ] ; then
  echo
  echo "Fresh miniconda installation."
  wget $URL -O miniconda.sh
  rm -rf $MINICONDA_DIR
  bash miniconda.sh -b -p $MINICONDA_DIR
fi

export PATH="$MINICONDA_DIR/bin:$PATH"

echo
echo "[show conda]"
which conda

echo
echo "[update_conda]"
conda update conda --yes
conda config --set show_channel_urls true
conda config --add channels conda-forge --force

echo
echo "[create env]"

conda create --yes -n TEST python=$TRAVIS_PYTHON_VERSION --file requirements.txt --file requirements-dev.txt
source activate TEST
