#!/usr/bin/env python3
"""Strip Jackson classes from fat jar to avoid classpath conflicts with Flink."""
import zipfile
import os
import sys

SRC = r'g:\Projects\PersonalWebsite\delayed-order-sms-flink\job_7faa.jar'
DST = r'g:\Projects\PersonalWebsite\delayed-order-sms-flink\job_clean.jar'

JACKSON_PATTERNS = [
    'com/fasterxml',
    'jackson-core',
    'jackson-databind',
    'jackson-annotations',
    'jackson-datatype',
]

with zipfile.ZipFile(SRC, 'r') as zin:
    names = zin.namelist()
    stripped = 0
    with zipfile.ZipFile(DST, 'w', zipfile.ZIP_DEFLATED) as zout:
        for n in names:
            if any(p in n for p in JACKSON_PATTERNS):
                stripped += 1
                continue
            zout.writestr(n, zin.read(n))

print(f'Stripped {stripped}/{len(names)} Jackson entries -> {DST} ({os.path.getsize(DST)} bytes)')