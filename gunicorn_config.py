import multiprocessing

bind = "127.0.0.1:8000"

# Increasing the timeout because some sosreports can be big
# and take up to 15m-20m (or more?) to process.
timeout = "3600" # 3600 seconds is 60 minutes
#debug = True
workers = multiprocessing.cpu_count() * 2 + 1
