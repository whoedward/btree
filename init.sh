make clean
make
rm -f my_disk*
./makedisk my_disk 1024 1024 1 16 64 100 10 .28
./btree_init my_disk 10 4 4
./btree_insert my_disk 10 aasd 3324
./btree_insert my_disk 10 asdf 1234
./btree_insert my_disk 10 aafd 1322
./btree_insert my_disk 10 asdz 1231
./btree_display my_disk 10 dot
