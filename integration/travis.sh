#!/usr/bin/env bash

set -ex

mkdir travis
cd travis

sudo groupadd nobody

sudo /sbin/stop runsvdir
sudo sed -i -e 's;/usr/sbin/runsvdir-start;/usr/bin/runsvdir /var/service;g' /etc/init/runsvdir.conf
sudo /sbin/start runsvdir

wget https://dl.bintray.com/mitchellh/consul/0.4.1_linux_amd64.zip
unzip 0.4.1_linux_amd64.zip
rm 0.4.1_linux_amd64.zip

git clone https://github.com/square/prodeng
cd prodeng/nolimit
make
sudo cp nolimit /usr/bin
cd ../..
sudo cp prodeng/servicebuilder/bin/servicebuilder /usr/bin
sudo mkdir /etc/servicebuilder.d /var/service-stage /var/service

gpg --no-default-keyring --keyring $PWD/pubring.gpg --secret-keyring $PWD/secring.gpg --import <<EOF
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v1.4.13 (GNU/Linux)

mQENBFTdUNwBCACdZSiKNBwUWsEfBBqN4JGuKBx7n9kQkD8sTn/i/InIe4GwxAs8
R9/A07GNJe4WSkVA52T248MYzpCVjRAgM+u2OZHj8Y036dR5VoSIvtwy/OApXdU8
SqzqyKpMK3w5cq5WP+ZNAZURYJbmdsZSW4aoT2YbqdHEAbudRWCOwI07f5wyJzrp
dLjSR4pXmBZ5P9vB4vS5uvYRFuqud9Bg/+Z/EY3uDVm2gb+/yH+iwhjKQcEpdxF/
yd7jom3DDHoOWtM/n+WO8V+iz7+fwRAhU+F9Dn1Ksx3cZuMYkPWHeSIzAg/5wYkk
zZjBgsjZw/tGvc3f/yw5UnWHD8jS18I1I4B5ABEBAAG0JHAydW5pdmVyc2UgPGRl
cGxveS1kZXZAc3F1YXJldXAuY29tPokBOAQTAQIAIgUCVN1Q3AIbAwYLCQgHAwIG
FQgCCQoLBBYCAwECHgECF4AACgkQs6P0At4Z1foIOwf/f4JjQR/fmq/4sA4RIQz/
qYOYpoBwA6VgFzlOsAjiCqE/xESmKofLVQAI2U9/aTJmzyVkvAA9C/UwWnXw45qM
1z+/4ZFAVivvte7IAdXdvUdaa0mL092yWDivZBbymlIjTv2r8/pC6Zzxh59pPYmt
1RoITo41l9YoZsmHFl5g4RT8j3vBV4i8q8RzF2pNhEPOK02qOnLOtoqGcVDQ/sXT
CJLyewDF6OtkbrDVcT7Fru5gqgE+yx+6T4wYGySMNC8lhj0JYrYAuWWAAl9OgPfb
L/8P1tsUANWG3DAB6921csJhXarX2zj+Iqap7O1VnATZMpUejiKAUvFDQW2PCxLv
QLkBDQRU3VDcAQgAwqGSw1+BLcn/te1Dafda/Ahps+KVOC9nsec+q7+Ie2V4X6g+
Qz0M4idKZbZVg+CSeWi9CFJ25Ti2IzqhBRxh0MM7j4yvr7vy6hREl63BVTQRv3nY
KsmSJ2UhOWRx/QgeiIrGCuQwlaBulk+sFJ/nIv4XkebpZ4UkM0dL6Hx6O0ljQ9eV
dbrRr7nPAu6YKR6TzESAxIy48iC/6GdYmgFHYJTRrRW6S9N4WnVd2FjLIoRDiYnD
41G6P6jLILUfm8OGj/QqR6KQZRBEIm3c5qM5VI/AwoE1wEANo8eCro9DmwofIFMh
AhJP3+QxbNETzrpC5dEQj+odn8OD+IkAUDeRPQARAQABiQEfBBgBAgAJBQJU3VDc
AhsMAAoJELOj9ALeGdX6UCMH/iUmZqpJcyBi+4f7+s6SIT8SKh9IwM/klkxVoOsK
S4eNHQK+0JXSTGLEx9XxNpSAZSke50fGcmzxokpb+IADH+p3ck3ZMu08395XzEMl
4w3cmUu6pdN7urNVQBrdWwxyZ66i2Dh/Vd83QmZ5rwk6m5x0Ob97JsJ9KO/Dvqic
JtI7nqAppVaYI9JeeOdSqjtddoQmuxzy4qfjT8M+D4y9/4ZcQGXFiKC5HOMaaar6
25MG3OVT1k329oT9beauOdo3RqW8SjK/6y0Sy1ZfuZodCO43Ak7gYisYT1BgDsyx
xisJuc75Ar7leHrw+7hdBeptVCVEaT9yylSTuwxugikXBzI=
=XSs4
-----END PGP PUBLIC KEY BLOCK-----
EOF

gpg --no-default-keyring --keyring $PWD/pubring.gpg --secret-keyring $PWD/secring.gpg --allow-secret-key-import --import <<EOF
-----BEGIN PGP PRIVATE KEY BLOCK-----
Version: GnuPG v1.4.13 (GNU/Linux)

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
=I1Jf
-----END PGP PRIVATE KEY BLOCK-----
EOF

mkdir -p intent/bin
mv consul intent/bin
cat > intent/bin/launch <<'EOF'
#!/usr/bin/env bash
exec "${BASH_SOURCE[0]%/*}"/consul agent -data-dir $POD_HOME/consul_data -server -bootstrap-expect 1
EOF
chmod +x intent/bin/launch
tar -czvf intent.tar.gz -C intent ./

cat > intent.yaml <<EOF
id: intent
launchables:
  consul:
    launchable_type: hoist
    launchable_id: intent
    location: $PWD/intent.tar.gz
EOF

mkdir -p preparer/bin
cp $(which p2-preparer) preparer/bin/launch
tar -czvf preparer.tar.gz -C preparer ./

cat > preparer.yaml <<EOF
id: p2-preparer
launchables:
  p2-preparer:
    launchable_type: hoist
    launchable_id: p2-preparer
    location: $PWD/preparer.tar.gz
config:
  preparer:
    keyring: $PWD/pubring.gpg
EOF

gpg --no-default-keyring --keyring $PWD/pubring.gpg --secret-keyring $PWD/secring.gpg -u p2universe --output intent.yaml.asc --clearsign intent.yaml
gpg --no-default-keyring --keyring $PWD/pubring.gpg --secret-keyring $PWD/secring.gpg -u p2universe --output preparer.yaml.asc --clearsign preparer.yaml

sudo $(which p2-bootstrap) --consul-timeout 2m --consul-pod $PWD/intent.yaml.asc --agent-pod $PWD/preparer.yaml.asc
