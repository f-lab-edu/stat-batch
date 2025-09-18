pipeline {
  agent any
  stages {
    stage('Run Spring Batch') {
      steps {
        sh """
          docker pull ${DOCKERHUB_USERNAME:-kimsehee98}/stat-batch:latest
          docker run --rm ${DOCKERHUB_USERNAME:-kimsehee98}/stat-batch:latest
        """
      }
    }
  }
}
