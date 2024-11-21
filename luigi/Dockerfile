FROM python:3.9-slim

# Install required packages and Luigi
RUN pip install --no-cache-dir luigi minio
RUN pip install bs4
RUN pip install requests
RUN pip install pandas
RUN pip install numpy
RUN pip install streamlit
RUN pip install textblob


# Set a working directory
WORKDIR /app

# Expose the Luigi web server port
EXPOSE 8082

# Default command to run the Luigi web server
CMD ["luigid", "--port", "8082"]
