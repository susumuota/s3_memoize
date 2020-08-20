rm -rf s3_memoize.egg-info dist build
python setup.py sdist bdist_wheel
twine upload --repository pypi dist/*
