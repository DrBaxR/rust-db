# RustDB (name subject to change)

A DBMS implemented using Rust.

## Purpose

This system is not meant to be used in any sort of production environment, as I mainly implemented it as a way to lean more about the following topics:
- Databases
- Rust
- Concurrency
- I/O Optimizations

Basically, what I'm saying is that I have no clue what I'm doing and ***ALL*** the code in this repo can be improved in many different ways.

## Goal

The main goal I started in mind with when I started working on this project was to implement a piece of software that accepts multiple connections at the same time and accepts and runs SQL queries concurrently in a decently performant manner, and supports transactions.

## Resources

Here are a bunch of resources that I used in the process of developing this project:
1. The [CMU Intro To Database Systems](https://www.youtube.com/watch?v=vdPALZ-GCfI&list=PLSE8ODhjZXjbj8BMuIrRcacnQh20hmY9g) Course (If there were one resource that I can wholehartedly recommend for learning more about how databases work, it would be this course)
2. The [BusTub](https://github.com/cmu-db/bustub) Source Code (This is very useful if you follow along with the course)
3. Tony Saro's [Writing My Own Database From Scratch](https://www.youtube.com/watch?v=5Pc18ge9ohI&list=WL&index=16) Video (This is the video that made me start this project)
4. Mara Bos's [Rust Atomics and Locks](https://marabos.nl/atomics/) Book
5. The [PostgreSQL Documentation](https://www.postgresql.org/docs/17/index.html)
