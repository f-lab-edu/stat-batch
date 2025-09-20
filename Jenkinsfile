pipeline {
  agent any
  stages {
    stage('Run Spring Batch') {
      steps {
        sh '''
          docker pull ${DOCKERHUB_USERNAME:-kimsehee98}/stat-batch:latest
          docker run --rm ${DOCKERHUB_USERNAME:-kimsehee98}/stat-batch:latest \
            --job.name=statBatchJob aggregationDay=2025-09-04 chunkSize=2000
        '''
      }
    }
  }
}
