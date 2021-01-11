<template>
  <v-container fluid>
    <h1> Recommend System </h1>
    <v-divider></v-divider>
      <v-form
        ref="form"
    v-model="valid"
      >
      <v-text-field
        v-model="studentID"
        :rules="nameRules"
        label="Student ID"
        required
      ></v-text-field>
  
      <v-text-field
        v-model="number"
        :rules="numberRule"
        label="Recommendary Problems Number [Optional]"
      ></v-text-field>
  
      <v-select
        v-model="difficulties"
        multiple
        :items="['Easy', 'Normal', 'Hard']"
        label="Difficulties [Optional]"
      ></v-select>
      <v-btn
        color="success"
        class="mr-4"
        @click="submit"
      >
        Submit
      </v-btn>
    </v-form>
    <v-card-text v-html="dialogText"></v-card-text>
    <v-divider></v-divider>
    <div v-show="easy">
      <v-card>
        <v-card-title> Easy </v-card-title>
      </v-card>
        <v-data-table
        :headers="headers"
        :items="problems['easy']"
        :items-per-page="5"
        class="elevation-1"
      ></v-data-table>
    </div>
    
    <v-card-text v-html="dialogText"></v-card-text>
    <v-divider></v-divider>
    <div v-show="normal">
    <v-card>
      <v-card-title> Nomral </v-card-title>
    </v-card>
      <v-data-table
        :headers="headers"
        :items="problems['normal']"
        :items-per-page="5"
        class="elevation-1"
      ></v-data-table>
    </div>
    <v-card-text v-html="dialogText"></v-card-text>
    <v-divider></v-divider>
    <div v-show="hard">
    <v-card>
      <v-card-title> Hard </v-card-title>
    </v-card>
      <v-data-table
        :headers="headers"
        :items="problems['hard']"
        :items-per-page="5"
        class="elevation-1"
      ></v-data-table>
    </div>
  </v-container>
</template>

<script>
  export default {
    name: 'Recommend',
    data: function() {
      return {
		dialogText: "<br>",
                valid: true,
                recommendary_problems: [],
                nameRules: [
                        v => !!v || 'Student ID is required',
                ],
                numberRule: [
                   v => /^\d+$/.test(v) || 'Must be a number',
                 ],
                studentID: '',
                number: 10,
                difficulties: ['Normal'],
                easy: false,
                normal: false,
                hard: false,
                headers: [
                {
                  text: 'Problem Content',
                  align: 'start',
                  sortable: true,
                  value: 'content',
                },
                {
                  text: 'Possible Accuracy',
                  value: 'accuracy'
                }
                  ],
               problems: {
                  'easy': [],
                  'normal': [],
                  'hard': [],
               },
        }
    },
    methods: {
    submit () {
        var vm = this;
        vm.easy = false
        vm.normal = false
        vm.hard = false
        if(!this.$refs.form.validate()){
            return
        }
        this.$http.get('[Your API Host]', {headers: {
          'Access-Control-Allow-Origin': '*'
           },
          params: {
            student_id: vm.studentID,
            number: vm.number,
            difficulties: vm.difficulties.join(',')
          },
          
        })
          .then(function (response) {
              vm.problems = response.data.recommendary_problems
              if (vm.problems['easy'] === undefined || vm.problems['easy'].length == 0) {
                  vm.easy = false
              }
              else {
                  vm.easy = true
              }
              if (vm.problems['normal'] === undefined || vm.problems['normal'].length == 0) {
                  vm.normal = false
              }
              else {
                  vm.normal = true
              }
              if (vm.problems['hard'] === undefined || vm.problems['hard'].length == 0) {
                  vm.hard = false
              }
              else {
                  vm.hard = true
              }
          })
          .catch(function (error) {
              console.log(error);
          });
        },
    },
  }
</script>
