from setuptools import setup

setup(name='awslambdadecorators',
      version='0.2-beta2',
      description='Useful decorators AWS Lambda',
      url='https://github.com/hhagblom/lambda-decorators',
      author='Hans Peter Hagblom',
      author_email='hagblom.hp@gmail.com',
      license='None',
      packages=['awslambdadecorators'],
      zip_safe=False,
      install_requires=[
          'boto'
      ])
