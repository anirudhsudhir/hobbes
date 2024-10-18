#!/bin/zsh

for ((i = 0; i < 10000; i++)); do
  ./hobbes set foo "bar_$i"
done
