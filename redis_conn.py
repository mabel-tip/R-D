import redis

class RedisConnection(object):
    
    def __init__(self) -> None:
        pass

    def redis_connection(self, host=None):
        # return redis.Redis(host=REDIS_AWS_HOST, port=REDIS_AWS_PORT, password=REDIS_AWS_PASSWORD, decode_responses=True, db=0, socket_connect_timeout=60, socket_timeout=60, retry_on_timeout=True)
    
        # For local testing only
        return redis.Redis(host='localhost', port='6379', password=None, decode_responses=True, db=0, socket_connect_timeout=60, socket_timeout=60, retry_on_timeout=True)
        
    def redis_connection_pipeline(self):
        return self.redis_connection().pipeline()