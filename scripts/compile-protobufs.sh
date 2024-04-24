PROTO_PATH=protos
PROTO_GEN_PATH=grpc/pb

if [ ! -x "${PROTO_GEN_PATH}" ]; then
    mkdir -p "${PROTO_GEN_PATH}"
fi
# Clean-up existing auto-generated files
rg --files-with-matches "^// Code generated .* DO NOT EDIT\.$" ${PROTO_GEN_PATH} | xargs rm
find ${PROTO_GEN_PATH} -type d -empty -delete

for PROTO_FILE in "${PROTO_PATH}"/*.proto; do
    PACKAGE=$(basename $PROTO_FILE | sed -e 's/\.proto$//g')
    mkdir -p ${PROTO_GEN_PATH}/${PACKAGE}
    # Generate Proto Go Code
    protoc \
        --go_out="${PROTO_GEN_PATH}/${PACKAGE}" \
        --go_opt=paths=source_relative \
        --go-grpc_out="${PROTO_GEN_PATH}/${PACKAGE}" \
        --go-grpc_opt=paths=source_relative \
        --proto_path ${PROTO_PATH} \
        ${PROTO_FILE}
done