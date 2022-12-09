#!groovy

pipeline {
  agent {
    label "stork-builder"
  }

  stages {
    stage("Build Stork") {
      steps {
        build(job: "${CBT_STORK_BUILD_JOB_NAME}", parameters: [string(name: "GIT_BRANCH", value: GIT_BRANCH)])
      }
    }
    stage("Run Stork CBT tests") {
      steps {
        build(job: "${CBT_STORK_TEST_JOB_NAME}", parameters: [string(name: "GIT_BRANCH", value: GIT_COMMIT)])
      }
    }
  }
}
