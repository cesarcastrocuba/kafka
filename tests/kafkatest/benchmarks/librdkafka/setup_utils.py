# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import logging
import shutil
import tempfile
from ducktape.cluster.remoteaccount import RemoteCommandError

logger = logging.getLogger(__name__)

# librdkafka version to use - updated to 2.10.1
LIBRDKAFKA_VERSION = "2.10.1"

# Path to e2e_latency.c in the package
E2E_LATENCY_C_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "e2e_latency.c")

# Installation commands for different package managers
INSTALL_COMMANDS = {
    "apt": [
        "sudo apt-get update",
        "sudo DEBIAN_FRONTEND=noninteractive apt-get install -y gcc g++ make pkg-config git python3-dev libssl-dev zlib1g-dev libzstd-dev libsasl2-dev libsasl2-modules openssl krb5-user libsasl2-modules-gssapi-mit"
    ],
    "yum": [
        "sudo yum update -y",
        "sudo yum install -y gcc gcc-c++ make pkgconfig git python3-devel openssl-devel zlib-devel libzstd-devel cyrus-sasl-devel cyrus-sasl-lib openssl krb5-workstation cyrus-sasl-gssapi"
    ],
    "dnf": [
        "sudo dnf update -y",
        "sudo dnf install -y gcc gcc-c++ make pkgconfig git python3-devel openssl-devel zlib-devel libzstd-devel cyrus-sasl-devel cyrus-sasl-lib openssl krb5-workstation cyrus-sasl-gssapi"
    ],
    "zypper": [
        "sudo zypper refresh",
        "sudo zypper install -y gcc gcc-c++ make pkg-config git python3-devel libopenssl-devel zlib-devel libzstd-devel cyrus-sasl-devel cyrus-sasl openssl krb5-client cyrus-sasl-gssapi"
    ],
    "apk": [
        "sudo apk update",
        "sudo apk add gcc g++ make pkgconfig git python3-dev openssl-dev zlib-dev libzstd-dev cyrus-sasl-dev cyrus-sasl openssl krb5"
    ]
}

def detect_package_manager(node):
    """Detect the package manager on the node"""
    package_managers = {
        "apt-get": "apt",
        "yum": "yum",
        "dnf": "dnf",
        "zypper": "zypper",
        "apk": "apk"
    }
    
    for cmd, name in package_managers.items():
        try:
            node.account.ssh(f"which {cmd}", allow_fail=True)
            return name
        except RemoteCommandError:
            continue
    
    raise Exception("Could not detect package manager on node")

def install_dependencies(node):
    """Install dependencies required for librdkafka on the node"""
    package_manager = detect_package_manager(node)
    logger.info(f"Detected package manager: {package_manager}")
    
    # Pre-configuración automática para Debian/Ubuntu
    if package_manager == "apt":
        node.account.ssh("sudo bash -c 'echo \"krb5-config krb5-config/default_realm string EXAMPLE.COM\" > /tmp/krb5-debconf'")
        node.account.ssh("sudo bash -c 'debconf-set-selections /tmp/krb5-debconf'")

    for cmd in INSTALL_COMMANDS[package_manager]:
        logger.info(f"Running: {cmd}")
        node.account.ssh(cmd)
    
    logger.info("Dependencies installed successfully")

def build_librdkafka_on_node(node):
    """Build librdkafka on a single node and create a tarball of the installation"""
    logger.info(f"Building librdkafka {LIBRDKAFKA_VERSION} on node {node.account.hostname}")
    
    # Create temporary directory
    temp_dir = f"/tmp/librdkafka-{LIBRDKAFKA_VERSION}"
    install_dir = f"/tmp/librdkafka-install-{LIBRDKAFKA_VERSION}"
    tarball_path = f"/tmp/librdkafka-{LIBRDKAFKA_VERSION}-bin.tar.gz"
    
    node.account.ssh(f"sudo rm -rf {temp_dir} {install_dir} {tarball_path}")
    node.account.ssh(f"mkdir -p {temp_dir} {install_dir}")
    
    # Download and extract librdkafka
    download_cmd = f"curl -L https://github.com/edenhill/librdkafka/archive/v{LIBRDKAFKA_VERSION}.tar.gz | " \
                   f"tar xz --strip-components=1 -C {temp_dir}"
    node.account.ssh(download_cmd)
    
    # Configure and build librdkafka with custom prefix
    build_cmds = [
        # f"cd {temp_dir} && ./configure --prefix={install_dir}",
        f"cd {temp_dir} && CFLAGS=\"-O3 -march=native -flto\" CXXFLAGS=\"-O3 -march=native -flto\" LDFLAGS=\"-flto\" ./configure --prefix={install_dir}",
        f"cd {temp_dir} && make -j$(nproc)",
        f"cd {temp_dir} && sudo make install"
    ]
    
    for cmd in build_cmds:
        logger.info(f"Running: {cmd}")
        node.account.ssh(cmd)
    
    # Update shared library cache
    node.account.ssh("sudo ldconfig")

    # Build rdkafka_performance tool
    node.account.ssh(f"cd {temp_dir}/examples && make")
    node.account.ssh(f"mkdir -p {install_dir}/bin/")
    node.account.ssh(f"sudo cp {temp_dir}/examples/rdkafka_performance {install_dir}/bin/")
    
    # Create tarball of the installation
    node.account.ssh(f"cd {install_dir} && tar czf {tarball_path} .")
    
    logger.info(f"librdkafka built and packaged at {tarball_path}")
    return tarball_path

def upload_and_install_tarball(node, local_path):
    
    install_dir = "/usr"
    # Extract tarball to the installation directory
    logger.info(f"Installing librdkafka from tarball on {node.account.hostname}")
    node.account.ssh(f"sudo tar xzf {local_path} -C {install_dir}")
    node.account.ssh("sudo ldconfig")
    
    logger.info(f"librdkafka installed on {node.account.hostname}")

def install_e2e_latency(node, dest_dir):
    """Copy and compile e2e_latency.c on the node"""
    logger.info("Installing e2e_latency tool")
    
    # Create destination directory if it doesn't exist
    node.account.ssh(f"sudo mkdir -p {dest_dir}")
    node.account.ssh(f"sudo chmod 777 {dest_dir}")
    
    # Copy e2e_latency.c to the node
    dest_file = os.path.join(dest_dir, "e2e_latency.c")
    node.account.create_file(dest_file, open(E2E_LATENCY_C_PATH, 'r').read())
    
    # Compile e2e_latency
    compile_cmd = f"gcc -o {os.path.join(dest_dir, 'e2e_latency')} {dest_file} -lrdkafka"
    node.account.ssh(compile_cmd)
    
    logger.info("e2e_latency tool installed successfully")

def check_librdkafka_installed(node):
    """Check if librdkafka is installed by looking for the library files"""
    try:
        output = node.account.ssh_capture("find /usr/lib -name \"librdkafka*\"", allow_fail=True)
        for line in output:
            if "librdkafka.so" in line:
                return True
        return False
    except RemoteCommandError:
        return False

def setup_ssl_certificates(node, security_dir="/mnt/security"):
    """Generate and set up SSL certificates for librdkafka nodes"""
    logger.info(f"Setting up SSL certificates on {node.account.hostname}")
    
    # Create security directory if it doesn't exist
    node.account.ssh(f"mkdir -p {security_dir}")
    
    # Use client-specific filenames to avoid conflicts with broker certificates
    client_keystore = "librdkafka.keystore.jks"
    client_keystore_p12 = "librdkafka.keystore.p12"
    client_cert_pem = "librdkafka.certificate.pem"
    client_key_pem = "librdkafka.key.pem"
    
    # Define the paths to the certificate files
    truststore_pem = f"{security_dir}/test.truststore.pem"
    keystore_jks = f"{security_dir}/{client_keystore}"
    keystore_p12 = f"{security_dir}/{client_keystore_p12}"
    certificate_pem = f"{security_dir}/{client_cert_pem}"
    key_pem = f"{security_dir}/{client_key_pem}"
    
    # Remove existing certificate files if they exist
    try:
        for file_path in [truststore_pem, keystore_jks, keystore_p12, certificate_pem, key_pem]:
            if node.account.exists(file_path):
                logger.info(f"Removing existing certificate file {file_path} on {node.account.hostname}")
                node.account.ssh(f"rm -f {file_path}")
    except Exception as e:
        logger.warning(f"Error removing existing certificate files: {e}")
        # Continue with certificate generation even if there was an error removing files

    # Define passwords
    keystore_password = "test-ks-passwd"
    truststore_password = "test-ts-passwd"
    
    # Get the hostname for SAN
    hostname = node.account.hostname

    # Export the CA certificate from the truststore to a PEM file for librdkafka
    # This assumes that the truststore already exists and contains the CA certificate
    node.account.ssh(f"""
        cd {security_dir} && \\
        # Export the CA certificate from the truststore to a PEM file for librdkafka
        keytool -exportcert -alias ca -keystore test.truststore.jks -rfc -file test.truststore.pem \\
        -storepass {truststore_password}
    """)
    
    # Create a client keystore with a certificate for this node, using systemtest as CN
    node.account.ssh(f"""
        cd {security_dir} && \\
        # Create a keystore with a certificate for this node
        keytool -genkeypair -alias kafka -keyalg RSA -keysize 2048 \\
        -keystore {client_keystore} -storepass {keystore_password} \\
        -keypass {keystore_password} -dname "CN=systemtest" \\
        -ext SAN=DNS:{hostname} -storetype pkcs12
    """)
    
    # Convert keystore to PEM format for librdkafka
    node.account.ssh(f"""
        cd {security_dir} && \\
        # Export keystore JKS to PKCS12
        keytool -importkeystore -srckeystore {client_keystore} -destkeystore {client_keystore_p12} \\
        -srcstoretype JKS -deststoretype PKCS12 -srcstorepass {keystore_password} \\
        -deststorepass {keystore_password} && \\
        
        # Convert PKCS12 to PEM (certificate and key)
        openssl pkcs12 -in {client_keystore_p12} -out {client_cert_pem} -nokeys \\
        -passin pass:{keystore_password} && \\
        openssl pkcs12 -in {client_keystore_p12} -out {client_key_pem} -nocerts -nodes \\
        -passin pass:{keystore_password}
    """)
    
    # Verify that the PEM files are correctly formatted
    node.account.ssh(f"""
        cd {security_dir} && \\
        openssl x509 -in {client_cert_pem} -text -noout && \\
        openssl rsa -in {client_key_pem} -check
    """)
    
    # Update the configure_security_settings function to use the new file paths
    logger.info(f"SSL certificates set up successfully on {node.account.hostname}")
    return True

def get_cipher_suites(tls_version=None):
    """
    Get the appropriate cipher suites based on the TLS version.
    
    Args:
        tls_version: TLS version to use (TLSv1.2, TLSv1.3, etc.)
        
    Returns:
        String with cipher suites for the specified TLS version
    """
    if tls_version == 'TLSv1.2':
        return 'ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384'
    elif tls_version == 'TLSv1.3':
        # For TLS 1.3, we can use the default OpenSSL configuration
        return None
    else:
        # Default cipher suites for both TLS 1.2 and 1.3
        return 'TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384'

def configure_security_settings(security_protocol='PLAINTEXT', tls_version=None):
    """
    Configure security settings for librdkafka based on the security protocol and TLS version.

    Args:
        security_protocol: Security protocol to use (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)
        tls_version: TLS version to use (TLSv1.2, TLSv1.3, etc.)
        
    Returns:
        Dictionary with security settings for librdkafka
    """
    settings = {
        'security.protocol': security_protocol.lower()
    }
    # Add SSL configuration if needed
    if security_protocol in ['SSL', 'SASL_SSL']:
        logger.info(f"Configuring SSL for librdkafka with protocol {security_protocol}")
        
        # Basic SSL configuration with updated file paths
        ssl_settings = {            
            'ssl.ca.location': '/mnt/security/test.truststore.pem',
            'ssl.certificate.location': '/mnt/security/librdkafka.certificate.pem',
            'ssl.key.location': '/mnt/security/librdkafka.key.pem',
            'ssl.key.password': 'test-ks-passwd'
        }
        
        # Add TLS version configuration
        if tls_version:
            cipher_suites = get_cipher_suites(tls_version)
            if cipher_suites is not None:
                logger.info(f"Setting TLS version to {tls_version}")
                ssl_settings['ssl.cipher.suites'] = cipher_suites
            
        # Log the SSL settings for debugging
        logger.info(f"SSL settings for librdkafka: {ssl_settings}")

        # Update settings with SSL configuration
        settings.update(ssl_settings)

    # Add SASL configuration if needed
    if 'SASL' in security_protocol:
        logger.info(f"Configuring SASL for librdkafka with protocol {security_protocol}")
        
        # Basic SASL configuration
        sasl_settings = {
            'sasl.mechanisms': 'GSSAPI',
            'sasl.kerberos.service.name': 'kafka',
            'sasl.kerberos.keytab': '/mnt/security/keytab',
            'sasl.kerberos.principal': 'client@EXAMPLE.COM'
        }
        
        # Log the SASL settings for debugging
        logger.info(f"SASL settings for librdkafka: {sasl_settings}")
        
        # Update settings with SASL configuration
        settings.update(sasl_settings)

    return settings

def setup_librdkafka_node(node, service_name="rdkafka", tarball_path=None):
    """
    Set up a single node with librdkafka and required tools
    
    Args:
        node: Node to set up
        service_name: Service name for directories
        setup_ssl: If True, configure SSL certificates
        tarball_path: Path to existing librdkafka tarball (optional)
        
    Returns:
        Tuple of (success, tarball_path) where success is True if setup was successful,
        and tarball_path is the path to the generated tarball (if any)
    """
    logger.info(f"Setting up librdkafka on node {node.account.hostname}")
    
    # Check if librdkafka is already installed
    librdkafka_installed = check_librdkafka_installed(node)
    
    # If librdkafka is not installed, install it
    if not librdkafka_installed:
        # Install dependencies
        install_dependencies(node)
        if tarball_path:
            # If we have a tarball, use it
            upload_and_install_tarball(node, tarball_path)
            generated_tarball = None
        else:
            # Otherwise, build librdkafka on this node
            generated_tarball = build_librdkafka_on_node(node)
            upload_and_install_tarball(node, generated_tarball)
    else:
        generated_tarball = None
    
    # Install e2e_latency tool
    dest_dir = f"/mnt/{service_name}"
    try:
        install_e2e_latency(node, dest_dir)
    except Exception as e:
        logger.error(f"Error installing e2e_latency on {node.account.hostname}: {e}")
        return False, generated_tarball
    
    return True, generated_tarball

def setup_librdkafka(nodes, service_name="rdkafka"):
    """
    Set up librdkafka on multiple nodes with optimized installation
    
    Args:
        nodes: List of nodes to set up
        service_name: Service name for directories
        
    Returns:
        True if setup was successful on all nodes, False otherwise
    """
    if not nodes:
        logger.error("No nodes provided to set up")
        return False
    
    logger.info(f"Setting up librdkafka on {len(nodes)} nodes")
    
    # Set up the first node and get the tarball
    first_node = nodes[0]
    success, tarball_path = setup_librdkafka_node(first_node, service_name)
    
    if not success:
        logger.error(f"Failed to set up librdkafka on first node {first_node.account.hostname}")
        return False
    
    # If we have more than one node, we need to distribute the tarball
    if len(nodes) > 1:
        logger.info(f"Distributing librdkafka tarball to {len(nodes)-1} additional nodes")
        
        # Instead of downloading locally and then uploading, transfer directly between nodes
        for node in nodes[1:]:
            try:
                node_success = False
                if tarball_path:
                    # Create a temporary directory on the target node
                    node.account.ssh("mkdir -p /tmp")
                
                    # Use scp to copy the tarball directly from the first node to the current node
                    # This requires that SSH keys are set up between nodes, which should be the case in ducktape
                    first_node.account.ssh(f"sudo scp {tarball_path} {node.account.hostname}:{tarball_path}")
                
                    # Verify the tarball exists on the target node
                    if not node.account.exists(tarball_path):
                        logger.error(f"Failed to copy tarball to {node.account.hostname}")
                        return False
                
                    # Now set up librdkafka on this node using the transferred tarball
                    node_success, _ = setup_librdkafka_node(node, service_name, tarball_path)
                else:
                    # If no tarball was generated, just set up librdkafka from scratch
                    node_success, _ = setup_librdkafka_node(node, service_name)
                
                if not node_success:
                    logger.error(f"Failed to set up librdkafka on node {node.account.hostname}")
                    return False
                
            except Exception as e:
                logger.error(f"Error setting up librdkafka on node {node.account.hostname}: {e}")
                return False
    
    return True