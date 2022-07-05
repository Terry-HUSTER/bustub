<img src="logo/bustub.svg" alt="BusTub Logo" height="200">

# Bustub

本仓库为 CMU 15445 fall 2020 的配套 lab,实现了一个简单的磁盘型数据库。

[课程主页及 lab guide](https://15445.courses.cs.cmu.edu/fall2020/assignments.html)

特点：
+ 支持多线程读写
+ 实现了缓冲池、B+ 树索引、查询计划、死锁检测、三层隔离级事务等特性

## 快速安装

1. 任意一个 Linux 发行版，安装好 `g++、cmake、make` 这几个 C++ 基本开发套件
2. 仓库 git clone 到本地
3. 编译
```bash
mkdir build
cd build
cmake ..
make
```
4. 使用 auto-test.h 进行本地评测
```bash
./auto-test.h -p 1 # -p 1 表示运行第 1 个 lab 的所有测试，具体用法可参考脚本源码
```

## 关于本地评测

fall 2020 的所有测试代码已经都集成到本地了，无需再提交到 gradescope 在线评测，只需要通过 auto-test.h 脚本即可使用。

这样做主要是考虑到多年后我可能还会跑一下这份代码，如果 gradescope 挂了那我就很尴尬了。

## 我是自学的新同学，该如何开始？

切到本仓库的 fall2020-local-test 分支，然后照着 lab guide 做就行，评测的话就用 auto-test.h 脚本。

如果有的地方实在想不出来就看看前人的实现吧。自学的人是没有助教帮忙 debug 的，只能自己想方法帮自己。

# 原版 README

BusTub is a relational database management system built at [Carnegie Mellon University](https://db.cs.cmu.edu) for the [Introduction to Database Systems](https://15445.courses.cs.cmu.edu) (15-445/645) course. This system was developed for educational purposes and should not be used in production environments.

**WARNING: IF YOU ARE A STUDENT IN THE CLASS, DO NOT DIRECTLY FORK THIS REPO. DO NOT PUSH PROJECT SOLUTIONS PUBLICLY. THIS IS AN ACADEMIC INTEGRITY VIOLATION AND CAN LEAD TO GETTING YOUR DEGREE REVOKED, EVEN AFTER YOU GRADUATE.**

## Cloning this repo

The following instructions will create a private BusTub that you can use for your development:

1. Go to [https://github.com/new](https://github.com/new) to create a new repo under your account. Pick a name (e.g. `private-bustub`) and make sure it is you select it as **private**.
2. On your development machine, clone the public BusTub:

   ```
   $ git clone --depth 1 https://github.com/cmu-db/bustub.git public-bustub
   ```
3. You next need to [mirror](https://git-scm.com/docs/git-push#Documentation/git-push.txt---mirror) the public BusTub repo into your own private BusTub repo. Suppose your GitHub name is `student` and your repo name is `private-bustub`, you will execute the following commands:

   ```
   $ cd public-bustub
   $ git push --mirror git@github.com:student/private-bustub.git
   ```

   This copies everything in the public BusTub repo into your own private repo. You can now delete this bustub directory:

   ```
   $ cd ..
   $ rm -rv public-bustub
   ```
4. Clone your own private repo on:

   ```
   $ git clone git@github.com:student/private-bustub.git
   ```
5. Add the public BusTub as a remote source. This will allow you to retrieve changes from the CMU-DB repository during the semester:

   ```
   $ git remote add public https://github.com/cmu-db/bustub.git
   ```
6. You can now pull in changes from the public BusTub as needed:

   ```
   $ git pull public master
   ```

We suggest working on your projects in separate branches. If you do not understand how Git branches work, [learn how](https://git-scm.com/book/en/v2/Git-Branching-Basic-Branching-and-Merging). If you fail to do this, you might lose all your work at some point in the semester, and nobody will be able to help you.

## Build

### Linux / Mac

To ensure that you have the proper packages on your machine, run the following script to automatically install them:

```
$ sudo build_support/packages.sh
```

Then run the following commands to build the system:

```
$ mkdir build
$ cd build
$ cmake ..
$ make
```

If you want to compile the system in debug mode, pass in the following flag to cmake:
Debug mode:

```
$ cmake -DCMAKE_BUILD_TYPE=Debug ..
$ make
```

This enables [AddressSanitizer](https://github.com/google/sanitizers), which can generate false positives for overflow on STL containers. If you encounter this, define the environment variable `ASAN_OPTIONS=detect_container_overflow=0`.

### Windows

If you are using Windows 10, you can use the Windows Subsystem for Linux (WSL) to develop, build, and test Bustub. All you need is to [Install WSL](https://docs.microsoft.com/en-us/windows/wsl/install-win10). You can just choose "Ubuntu" (no specific version) in Microsoft Store. Then, enter WSL and follow the above instructions.

If you are using CLion, it also [works with WSL](https://blog.jetbrains.com/clion/2018/01/clion-and-linux-toolchain-on-windows-are-now-friends).

## Testing

```
$ cd build
$ make check-tests
```

## Build environment

If you have trouble getting cmake or make to run, an easy solution is to create a virtual container to build in. There are two options available:

### Vagrant

First, make sure you have Vagrant and Virtualbox installed

```
$ sudo apt update
$ sudo apt install vagrant virtualbox
```

From the repository directory, run this command to create and start a Vagrant box:

```
$ vagrant up
```

This will start a Vagrant box running Ubuntu 20.02 in the background with all the packages needed. To access it, type

```
$ vagrant ssh
```

to open a shell within the box. You can find Bustub's code mounted at `/bustub` and run the commands mentioned above like normal.

### Docker

First, make sure that you have docker installed:

```
$ sudo apt update
$ sudo apt install docker
```

From the repository directory, run these commands to create a Docker image and container:

```
$ docker build . -t bustub
$ docker create -t -i --name bustub -v $(pwd):/bustub bustub bash
```

This will create a Docker image and container. To run it, type:

```
$ docker start -a -i bustub
```

to open a shell within the box. You can find Bustub's code mounted at `/bustub` and run the commands mentioned above like normal.
