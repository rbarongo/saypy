FROM python:3.12-slim

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install python dependencies first so this layer is cached across rebuilds
# that only touch application code.
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . /app/

# Where uploaded evidence files are written (see app/settings.py's upload_dir)
# — created here so it exists with the right ownership even before the app's
# first write, and to make the intended mount point obvious.
RUN mkdir -p /app/uploads

EXPOSE 8001

RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]

# Dev/prod compose files both override this with their own `command:` (with
# --reload, or with --workers) — this default is only used if the image is
# run standalone.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
