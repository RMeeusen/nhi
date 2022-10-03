setlocal

:: activate conda environment created by environment.yml
call conda activate nhi_docs

:: convert csv master table to .rst files
call python %~dp0\convert_master_table.py

:: Build .rst files to a html pages
cd ..
call sphinx-build -a -d "docs\_build\docs_doctree" docs "docs\_build\docs_out" --color -bhtml

pause 