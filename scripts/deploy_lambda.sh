#!/bin/bash

# El primer argumento que le pases al script será el nombre de la Lambda
LAMBDA_NAME=$1
ZIP_NAME="deploy_package.zip"

# 1. Validar que se haya pasado un argumento
if [ -z "$LAMBDA_NAME" ]; then
    echo "Error: Debes especificar el nombre de la Lambda."
    echo "Uso: ./scripts/deploy_lambda.sh <nombre_de_la_carpeta_lambda>"
    exit 1
fi

# 2. Validar que la carpeta de la Lambda exista (asumiendo que están en 'lambdas/')
TARGET_DIR="../Lambda/$LAMBDA_NAME"

if [ ! -d "$TARGET_DIR" ]; then
    echo "Error: El directorio $TARGET_DIR no existe."
    exit 1
fi

echo "==================================================="
echo "Iniciando despliegue para la Lambda: $LAMBDA_NAME"
echo "==================================================="

# Limpiar zip anterior en la raíz si por algún motivo quedó colgado
if [ -f "$ZIP_NAME" ]; then
    rm "$ZIP_NAME"
fi

echo "[1/2] Empaquetando todo el contenido de $TARGET_DIR..."

# Entramos a la carpeta de la Lambda seleccionada
cd "$TARGET_DIR" || exit

# Comprimimos TODO (.) lo que haya en el directorio actual.
# Se excluyen carpetas basura de Python, git y metadatos de sistema (como los de Mac).
zip -r "../../$ZIP_NAME" . -x "*.git*" "*/__pycache__/*" "*.DS_Store" ".*" > /dev/null

# Volvemos a la raíz del proyecto
cd ../../

# Comprobar si el zip se creó correctamente
if [ ! -f "$ZIP_NAME" ]; then
    echo "Error: Fallo al crear el archivo .zip"
    exit 1
fi

echo "[2/2] Subiendo el código a AWS Lambda..."
aws lambda update-function-code \
--function-name "$LAMBDA_NAME" \
--zip-file "fileb://$ZIP_NAME" \
--no-cli-pager > /dev/null

# Comprobar si la subida fue exitosa
if [ $? -eq 0 ]; then
    echo "¡Despliegue de $LAMBDA_NAME completado con éxito!"
else
    echo "Error al desplegar en AWS. Verifica tus credenciales o si el nombre coincide."
fi

# Limpieza final del archivo temporal
rm "$ZIP_NAME"