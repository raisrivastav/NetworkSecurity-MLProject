import ssl
import socket
import certifi

hosts = [
    "ac-qfjeibg-shard-00-00.1fby1cw.mongodb.net",
    "ac-qfjeibg-shard-00-01.1fby1cw.mongodb.net",
    "ac-qfjeibg-shard-00-02.1fby1cw.mongodb.net",
]

ctx = ssl.create_default_context()
ctx.check_hostname = True
ctx.verify_mode = ssl.CERT_REQUIRED
ctx.load_verify_locations(certifi.where())
# Force TLS 1.2 (Atlas requires TLS 1.2+)
try:
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
except AttributeError:
    pass

for h in hosts:
    print("connecting", h)
    with socket.create_connection((h, 27017), timeout=10) as sock:
        with ctx.wrap_socket(sock, server_hostname=h) as ssock:
            print("cipher", ssock.cipher())
            print("subject", ssock.getpeercert().get('subject'))
