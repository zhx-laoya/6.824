#!/bin/bash

# 初始化计数器
success_count=0

for ((i=1; i<=50; i++)); do
    # 执行测试命令
    go test -run 2A -race 
    # go test -run TestInitialElection2A

    # 检查测试结果
    if [ $? -eq 0 ]; then
        # 测试成功
        success_count=$((success_count + 1))
    fi

    # 每执行 10 次输出一次成功执行的次数
    if [ $((i % 10)) -eq 0 ]; then
        echo "成功执行的次数：$success_count"
    fi
done