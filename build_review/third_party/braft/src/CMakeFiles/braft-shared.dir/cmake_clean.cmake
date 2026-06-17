file(REMOVE_RECURSE
  "../output/lib/libbraft.dylib"
  "../output/lib/libbraft.pdb"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/braft-shared.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
