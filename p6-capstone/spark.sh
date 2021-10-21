#!/bin/bash

docker run -itd \
    -p 8888:8888 \
    -p 4040:4040 \
    -v ~/.aws:/root/.aws:ro \
    --name glue \
    amazon/aws-glue-libs:glue_libs_1.0.0_image_01
