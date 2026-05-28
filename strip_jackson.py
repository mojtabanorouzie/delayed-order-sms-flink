#!/usr/bin/env python3
"""Strip Jackson classes from fat jar to avoid classpath conflicts with Flink."""
import argparse
import zipfile
import os

def main():
    parser = argparse.ArgumentParser(description='Strip Jackson from fat JAR')
    parser.add_argument('src', help='Source JAR path')
    parser.add_argument('--output', '-o', help='Output JAR path', default=None)
    args = parser.parse_args()

    src = args.src
    dst = args.output if args.output else src.replace('.jar', '_clean.jar')

    JACKSON_PATTERNS = ['com/fasterxml', 'jackson-core', 'jackson-databind',
                        'jackson-annotations', 'jackson-datatype']

    with zipfile.ZipFile(src, 'r') as zin:
        names = zin.namelist()
        stripped = 0
        with zipfile.ZipFile(dst, 'w', zipfile.ZIP_DEFLATED) as zout:
            for n in names:
                if any(p in n for p in JACKSON_PATTERNS):
                    stripped += 1
                    continue
                zout.writestr(n, zin.read(n))

    print(f'Stripped {stripped}/{len(names)} Jackson entries -> {dst} ({os.path.getsize(dst)} bytes)')

if __name__ == '__main__':
    main()