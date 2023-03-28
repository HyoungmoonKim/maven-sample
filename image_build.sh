#!/bin/bash
set -e

#----------------------------------------------------------------------
# Help
function _help() {
echo "
Usage: $0 [-h] [-y] [-o output_dir]

Options:
  -h,             このヘルプ メッセージを表示して終了します
  -y,             すべての問い合わせに「yes」で答えます
  -o [output_dir] DockerImageファイルを[output_dir]へ保存します
" 1>&2
exit 1
}

#----------------------------------------------------------------------
# The selected item will be set to the "_SELECTED_ITEM" variable.
function _menu() {

  # menu items
  local _menu_dir=$1
  local _items=$(/bin/ls -d ${_menu_dir}/* |gawk -F/ '{print $NF}')
  _items+=("quit")

  # select image
  PS3="select number > "
  select _item in ${_items[@]}
  do
   case ${_item} in
     "")
       echo "Please enter the menu number." >&2
       continue
       ;;
     quit)
       exit
       ;;
     *)
       _SELECTED_ITEM=${_item}
       break
       ;;
   esac
  done

}

#----------------------------------------------------------------------
# main
#----------------------------------------------------------------------
set -e

#-- Parse Option
ASSUME_YES=0
while getopts o:hy OPT ;do
  case ${OPT} in
  o)  OUTPUT_DIR=${OPTARG} ;;
  y)  ASSUME_YES=1 ;;
  h)  _help ;;
  \?) _help ;;
  esac
done

#-- Check the build directory of 'BUILD_HOME'
IMAGE_HOME=$(cd $(dirname $0);pwd)/../../../../src/docker-images
BUILD_HOME=${BUILD_HOME:-${IMAGE_HOME}}
if [ ! -d ${BUILD_HOME} ]; then
  echo "Path of 'BUILD_HOME' does not directory." >&2
  echo "BUILD_HOME=${BUILD_HOME}" >&2
  exit 2
fi

#-- Select Image-name
MENU_CURRENT=$(cd ${BUILD_HOME};pwd)
[ "x${IMAGE_NAME}" != "x" ] && { MENU_CURRENT=$(cd ${BUILD_HOME};pwd)/${IMAGE_NAME} ;}
while [ ! -f ${MENU_CURRENT}/Dockerfile.${IMAGE_NAME} ]
do
  _menu ${MENU_CURRENT}
  if [ "x${IMAGE_NAME}" == "x" ]; then
    IMAGE_NAME=${_SELECTED_ITEM}
  fi
  MENU_CURRENT=${MENU_CURRENT}/${_SELECTED_ITEM}
done

#-- Check Version file
VERSION_FILE=${MENU_CURRENT}/VERSION
if [ ! -f ${VERSION_FILE} ];then
  echo "Version file is not found.[${VERSION_FILE}]" >&2
  exit 2
fi
TAG=$(cat ${VERSION_FILE})

#-- Confirm Yes/No
if [ ${ASSUME_YES} == 0 ];then
  echo "
  *** Is this OK? ***
  IMAGE_NAME=${IMAGE_NAME}
  TAG=${TAG}
  "
  read -n1 -p "ok? (y/N): " yn; [[ $yn = [yY] ]] && { echo; } || { exit 2; }
fi

#-- docker build
tar -C ${MENU_CURRENT} -cf - ./ ../../python 2>/dev/null | sudo docker build \
  --force-rm \
  --no-cache \
  --tag="${IMAGE_NAME}:${TAG}" \
  --file="Dockerfile.${IMAGE_NAME}" -

#-- docker save
OUTPUT_DIR=${OUTPUT_DIR:-.}
if [ ${ASSUME_YES} == 0 ];then
  echo -n "docker save ? [Y/n] "
  read _answer
else
  _answer="Y"
fi
if [ "${_answer}" == "Y" ];then
  _tarball=${OUTPUT_DIR}/${IMAGE_NAME}_${TAG}.tar.gz
  sudo docker save ${IMAGE_NAME}:${TAG} | gzip -c > ${_tarball}
  ls -l ${_tarball}
fi
exit 0
