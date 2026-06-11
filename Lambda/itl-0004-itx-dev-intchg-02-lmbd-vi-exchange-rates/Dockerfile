# =============================================================================
# Dockerfile — Lambda VISA Exchange Rates
# Base image oficial de AWS para Lambda con Python 3.12
# Incluye: Playwright + Chromium + BeautifulSoup4
# =============================================================================

FROM public.ecr.aws/lambda/python:3.12

# --- Dependencias del sistema necesarias para Chromium ---
RUN dnf install -y \
    atk \
    cups-libs \
    gtk3 \
    libXcomposite \
    libXcursor \
    libXdamage \
    libXext \
    libXi \
    libXrandr \
    libXScrnSaver \
    libXtst \
    pango \
    xorg-x11-fonts-100dpi \
    xorg-x11-fonts-75dpi \
    xorg-x11-fonts-cyrillic \
    xorg-x11-fonts-misc \
    xorg-x11-fonts-Type1 \
    xorg-x11-utils \
    alsa-lib \
    && dnf clean all

# --- Ruta fija para Chromium (debe declararse ANTES de instalar) ---
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# --- Dependencias Python ---
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- Instalar solo chromium headless shell (más liviano que Chromium completo) ---
RUN mkdir -p /ms-playwright && \
    playwright install chromium && \
    chmod -R 755 /ms-playwright

# --- Pre-calentar Chromium para reducir cold start ---
RUN python3 -c "from playwright.sync_api import sync_playwright; p = sync_playwright().start(); b = p.chromium.launch(); b.close(); p.stop()" || true

# --- Código del handler ---
COPY handler.py ${LAMBDA_TASK_ROOT}

# --- Entry point de Lambda ---
CMD ["handler.lambda_handler"]
