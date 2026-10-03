#!/bin/sh

PATH=/opt/homebrew/bin:$PATH

wget --user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36" -O gcd-dump-$1.zip $2
