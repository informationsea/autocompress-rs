#!/bin/bash

set -xeu -o pipefail

cd "$(dirname "$0")"


function compress() {
    ORIGINAL=$1
    gzip -c ${ORIGINAL} > ${ORIGINAL}.gz
    bzip2 -c ${ORIGINAL} > ${ORIGINAL}.bz2
    xz -c ${ORIGINAL} > ${ORIGINAL}.xz
    zstd -c ${ORIGINAL} > ${ORIGINAL}.zst
    bgzip -c ${ORIGINAL} > ${ORIGINAL}.bgzip.gz
    pigz -c ${ORIGINAL} > ${ORIGINAL}.pigz.gz
    gzip -c < ${ORIGINAL} > ${ORIGINAL}.pipe.gz

    mkdir -p tmp
    split -d -b 100k -a 3 ${ORIGINAL} tmp/${ORIGINAL}_

    for one in gz bz2 xz zst; do
        test -e ${ORIGINAL}.multistream.${one} && rm ${ORIGINAL}.multistream.${one}
    done

    for one in $(find tmp -type f|sort); do
        gzip -c ${one} >> ${ORIGINAL}.multistream.gz
        bzip2 -c ${one} >> ${ORIGINAL}.multistream.bz2
        xz -c ${one} >> ${ORIGINAL}.multistream.xz
        zstd -c ${one} >> ${ORIGINAL}.multistream.zst
    done

    rm -rf tmp
}


compress pg2701.txt