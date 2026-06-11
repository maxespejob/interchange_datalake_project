#!/bin/bash
set -e # Detiene el script si cualquier comando falla

# =============================================================================
# CONFIGURACIÓN GENERAL
# =============================================================================
LAMBDA_NAME=$1
ZIP_NAME="deploy_package.zip"

# ¡Tus variables personalizadas!
AWS_REGION="eu-south-2"

# Obtenemos el ID de cuenta usando tu perfil
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

if [ -z "$LAMBDA_NAME" ]; then
    echo "Error: Debes especificar el nombre de la Lambda."
    echo "Uso: ./scripts/deploy_lambda.sh <nombre_de_la_carpeta_lambda>"
    exit 1
fi

TARGET_DIR="../Lambda/$LAMBDA_NAME"

if [ ! -d "$TARGET_DIR" ]; then
    echo "Error: El directorio $TARGET_DIR no existe."
    exit 1
fi

echo "==================================================="
echo "Iniciando despliegue para: $LAMBDA_NAME"
echo "==================================================="

# Entramos a la carpeta de la Lambda
cd "$TARGET_DIR" || exit

# =============================================================================
# FLUJO 1: DESPLIEGUE CON DOCKER (Contenedores)
# =============================================================================
if [ -f "Dockerfile" ]; then
    echo "[1/5] Dockerfile detectado. Preparando entorno Docker..."
    
    ECR_REPO_NAME="itl-0004-itx-dev-lambda_layer_visa_exchange_rates"
    IMAGE_TAG="latest"
    ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}"

    echo "[2/5] Verificando repositorio ECR: ${ECR_REPO_NAME}..."
    aws ecr describe-repositories --repository-names "${ECR_REPO_NAME}" --region "${AWS_REGION}"  > /dev/null 2>&1 || \
    aws ecr create-repository --repository-name "${ECR_REPO_NAME}" --region "${AWS_REGION}"  > /dev/null
    
    echo "[3/5] Autenticando Docker en ECR..."
    aws ecr get-login-password --region "${AWS_REGION}"  | docker login --username AWS --password-stdin "$ECR_URI" > /dev/null

    echo "[4/5] Construyendo imagen Docker (linux/amd64)..."
    docker buildx build \
        --platform linux/amd64 \
        --provenance=false \
        -t "${ECR_REPO_NAME}:${IMAGE_TAG}" \
        . > /dev/null

    echo "[5/5] Subiendo imagen y actualizando Lambda..."
    docker tag "${ECR_REPO_NAME}:${IMAGE_TAG}" "${ECR_URI}:${IMAGE_TAG}"
    docker push "${ECR_URI}:${IMAGE_TAG}" > /dev/null

    # Actualizamos la Lambda (Asume que la Lambda ya fue creada por Terraform/Consola)
    aws lambda update-function-code \
        --function-name "$LAMBDA_NAME" \
        --image-uri "${ECR_URI}:${IMAGE_TAG}" \
        --region "$AWS_REGION" \
        --no-cli-pager > /dev/null

    echo "Esperando que Lambda termine de actualizarse..."
    aws lambda wait function-updated \
        --function-name "$LAMBDA_NAME" \
        --region "$AWS_REGION" 
fi

echo "==================================================="
echo "¡Despliegue de $LAMBDA_NAME completado con éxito!"
echo "==================================================="