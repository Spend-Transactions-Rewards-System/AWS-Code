@REM https://levelup.gitconnected.com/how-to-access-a-docker-container-from-another-container-656398c93576
@REM docker pull amazon/aws-glue-libs:glue_libs_3.0.0_image_01
@REM https://blog.datamics.com/how-to-install-pyspark-on-windows-faf7ac293ecf
docker run -it -v ~/.aws:/home/glue_user/.aws -v "%cd%:/home/glue_user/workspace/jupyter_workspace/" -e AWS_PROFILE=$PROFILE_NAME -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 -p 8998:8998 -p 8888:8888 --name glue_jupyter_lab amazon/aws-glue-libs:glue_libs_3.0.0_image_01 /home/glue_user/jupyter/jupyter_start.sh