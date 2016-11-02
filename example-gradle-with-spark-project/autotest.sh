#!/bin/sh

./gradlew -t --daemon -Ptags=@dev $* cucumber
