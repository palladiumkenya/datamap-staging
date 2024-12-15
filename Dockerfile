FROM python:3.11

# Set working directory
WORKDIR /app

# Copy application code and requirements
COPY requirements.txt /app/
COPY . /app/

# Install dependencies
RUN pip install --upgrade pip
RUN pip install flower
RUN pip install --no-cache-dir -r requirements.txt

# Expose ports
EXPOSE 9000 5555 9001 5772

# Command to run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9000"]
