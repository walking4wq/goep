D:\coding\ztesoft\golang\goep>git add ./web/

D:\coding\ztesoft\golang\goep>git commit -m "init version"
[master c68700c] init version
 1 file changed, 177 insertions(+)
 create mode 100644 web/index.html

D:\coding\ztesoft\golang\goep>git remote add origin https://github.com/walking4wq/goep/
fatal: remote origin already exists.

D:\coding\ztesoft\golang\goep>git push -f origin master
Username for 'https://github.com': walking4wq
Password for 'https://walking4wq@github.com':
Counting objects: 26, done.
Delta compression using up to 4 threads.
Compressing objects: 100% (22/22), done.
Writing objects: 100% (26/26), 18.36 KiB | 0 bytes/s, done.
Total 26 (delta 2), reused 0 (delta 0)
remote: Resolving deltas: 100% (2/2), done.
To https://github.com/walking4wq/goep.git
 + 0808d46...c68700c master -> master (forced update)



git init
git pull https://github.com/walking4wq/goep/ master
