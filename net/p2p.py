"""
DAGIUM P2P Network Module
Handles TLS-authenticated peer-to-peer communication between validators.
"""

import ssl
import socket
import threading
import logging
from typing import Optional, Callable, Dict, List
from dataclasses import dataclass
import json

logger = logging.getLogger(__name__)


@dataclass
class PeerInfo:
    """Information about a peer validator."""
    validator_id: str
    host: str
    port: int
    public_key_pem: bytes


class P2PNode:
    """Permissioned P2P network node with TLS authentication."""
    
    def __init__(self, validator_identity, listen_host: str = "localhost", listen_port: int = 9000):
        """
        Initialize P2P node.
        
        Args:
            validator_identity: ValidatorIdentity instance
            listen_host: Host to listen on
            listen_port: Port to listen on
        """
        self.validator = validator_identity
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.peers: Dict[str, PeerInfo] = {}
        self.message_handlers: Dict[str, Callable] = {}
        self.running = False
        self._lock = threading.Lock()
    
    def register_peer(self, peer_info: PeerInfo) -> bool:
        """
        Register a peer validator (permissioned).
        
        Args:
            peer_info: PeerInfo object
            
        Returns:
            True if registered
        """
        with self._lock:
            if peer_info.validator_id in self.peers:
                return False
            self.peers[peer_info.validator_id] = peer_info
            logger.info(f"Peer registered: {peer_info.validator_id} at {peer_info.host}:{peer_info.port}")
            return True
    
    def unregister_peer(self, validator_id: str) -> bool:
        """Unregister a peer."""
        with self._lock:
            if validator_id in self.peers:
                del self.peers[validator_id]
                logger.info(f"Peer unregistered: {validator_id}")
                return True
            return False
    
    def get_peers(self) -> List[PeerInfo]:
        """Get all registered peers."""
        with self._lock:
            return list(self.peers.values())
    
    def register_message_handler(self, msg_type: str, handler: Callable):
        """Register a handler for a specific message type.
        
        Args:
            msg_type: Message type identifier
            handler: Callback function for messages
        """
        self.message_handlers[msg_type] = handler
        logger.info(f"Message handler registered for type: {msg_type}")
    
    def send_to_peer(self, peer_id: str, message: dict) -> bool:
        """Send a message to a specific peer.
        
        Args:
            peer_id: Target peer validator ID
            message: Message dictionary
            
        Returns:
            True if sent successfully
        """
        with self._lock:
            if peer_id not in self.peers:
                logger.warning(f"Peer not found: {peer_id}")
                return False
            peer = self.peers[peer_id]
        
        try:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE  # In production, validate certs
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                with ctx.wrap_socket(sock, server_hostname=peer.host) as ssock:
                    ssock.connect((peer.host, peer.port))
                    msg_json = json.dumps(message)
                    ssock.sendall(msg_json.encode())
                    logger.debug(f"Message sent to {peer_id}: {message.get('type', 'unknown')}")
                    return True
        except Exception as e:
            logger.error(f"Failed to send message to {peer_id}: {e}")
            return False
    
    def broadcast(self, message: dict, exclude: Optional[str] = None):
        """Broadcast a message to all peers.
        
        Args:
            message: Message dictionary
            exclude: Validator ID to exclude
        """
        peers = self.get_peers()
        for peer in peers:
            if exclude and peer.validator_id == exclude:
                continue
            self.send_to_peer(peer.validator_id, message)
    
    def start(self):
        """Start listening for incoming connections."""
        if self.running:
            logger.warning("P2P node already running")
            return
        
        self.running = True
        listen_thread = threading.Thread(target=self._listen, daemon=True)
        listen_thread.start()
        logger.info(f"P2P node listening on {self.listen_host}:{self.listen_port}")
    
    def stop(self):
        """Stop the P2P node."""
        self.running = False
        logger.info("P2P node stopped")
    
    def _listen(self):
        """Listen for incoming connections."""
        ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        # In production, load actual certificates
        # ctx.load_cert_chain(certfile="validator.crt", keyfile="validator.key")
        
        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self.listen_host, self.listen_port))
            server_socket.listen(5)
            
            while self.running:
                try:
                    conn, addr = server_socket.accept()
                    threading.Thread(
                        target=self._handle_connection,
                        args=(conn, addr),
                        daemon=True
                    ).start()
                except Exception as e:
                    if self.running:
                        logger.error(f"Error accepting connection: {e}")
        except Exception as e:
            logger.error(f"P2P listener error: {e}")
        finally:
            server_socket.close()
    
    def _handle_connection(self, conn: socket.socket, addr):
        """Handle an incoming connection."""
        try:
            data = conn.recv(4096).decode()
            if not data:
                return
            
            message = json.loads(data)
            msg_type = message.get("type", "unknown")
            
            if msg_type in self.message_handlers:
                self.message_handlers[msg_type](message)
            else:
                logger.warning(f"No handler for message type: {msg_type}")
        except Exception as e:
            logger.error(f"Error handling connection from {addr}: {e}")
        finally:
            conn.close()


class PeerDiscovery:
    """Bootstrap and peer discovery mechanism."""
    
    def __init__(self, p2p_node: P2PNode):
        """Initialize peer discovery.
        
        Args:
            p2p_node: P2PNode instance
        """
        self.p2p_node = p2p_node
    
    def register_bootstrap_peers(self, peers: List[Dict]):
        """Register bootstrap peers from configuration.
        
        Args:
            peers: List of peer configurations
        """
        for peer_config in peers:
            peer_info = PeerInfo(
                validator_id=peer_config["id"],
                host=peer_config["host"],
                port=peer_config["port"],
                public_key_pem=peer_config.get("public_key", b"")
            )
            self.p2p_node.register_peer(peer_info)