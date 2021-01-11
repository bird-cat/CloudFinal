<template>
  <v-container fluid>
    <h1> Analytics Results </h1>
    <v-card-text v-html="dialogText"></v-card-text>
    <v-divider></v-divider>
    <v-row>
        <v-col>
        <h2> Total accuracy of execises: {{total_acc}} </h2>
        </v-col>
    </v-row>
    <v-divider></v-divider>
    <v-row>
        <v-col>
            <h2> {{name[0]}}</h2>
            <ve-histogram width="450px" :data="chartData['stu_dist']" :extend="extend"></ve-histogram>
        </v-col>
        <v-col>
            <h2> {{name[1]}} </h2>
            <ve-histogram width="450px" :data="chartData['solved_problem']"></ve-histogram>
        </v-col>
    </v-row>
    <v-card-text v-html="dialogText"></v-card-text>
    <v-divider></v-divider>
    <v-row>
        <v-col>
            <h2> {{name[2]}} </h2>
            <ve-line width="450px" :data="chartData['problem_acc']" :extend="extend"></ve-line>
        </v-col>
        <v-col>
            <h2> {{name[3]}} </h2>
            <ve-histogram width="450px" :data="chartData['diff_acc']" :extend="extend"></ve-histogram>
        </v-col>
    </v-row>
    <v-card-text v-html="dialogText"></v-card-text>
    <v-divider></v-divider>
    <v-row>
        <v-col>
            <h2> {{name[4]}} </h2>
            <ve-pie width="450px" :data="chartData['active_stu']" :extend="extend"></ve-pie>
        </v-col>
        <v-col>
            <h2> {{name[5]}} </h2>
            <ve-ring width="450px" :data="chartData['stu_month']" :extend="extend"></ve-ring>
        </v-col>
    </v-row>
    <v-card-text v-html="dialogText"></v-card-text>
    <v-divider></v-divider>
    <v-row>
        <v-col>
            <h2> {{name[6]}} </h2>
            <ve-histogram width="450px" :data="chartData['ele_week']" :extend="extend"></ve-histogram>
        </v-col>
        <v-col>
            <h2> {{name[7]}} </h2>
            <ve-histogram width="450px" :data="chartData['ele_weekend']" :extend="extend"></ve-histogram>
        </v-col>
    </v-row>
    <v-row>
        <v-col>
            <h2> {{name[8]}} </h2>
            <ve-histogram width="450px" :data="chartData['jun_week']" :extend="extend"></ve-histogram>
        </v-col>
        <v-col>
            <h2> {{name[9]}} </h2>
            <ve-histogram width="450px" :data="chartData['jun_weekend']" :extend="extend"></ve-histogram>
        </v-col>
    </v-row>
    <v-row>
        <v-col>
            <h2> {{name[10]}} </h2>
            <ve-histogram width="450px" :data="chartData['sen_week']" :extend="extend"></ve-histogram>
        </v-col>
        <v-col>
            <h2> {{name[11]}} </h2>
            <ve-histogram width="450px" :data="chartData['sen_weekend']" :extend="extend"></ve-histogram>
        </v-col>
    </v-row>
  </v-container>
</template>

<script>
  export default {
    name: 'Analytics',

    data: () => ({
        extend: {
          series: {
            label: { show: true, position: "top" }
          }
        },
        dialogText: "<br>",
        chartData: [
        ],
        total_acc: '',
        name: [],
    }),
    mounted() {
        var vm = this;
        this.$http.get('[Your API Host]', {headers: {
          'Access-Control-Allow-Origin': '*'
           },})
          .then(function (response) {
              vm.chartData = response.data.data
              vm.total_acc = response.data.total_acc
              vm.name = response.data.name
          })
          .catch(function (error) {
              console.log(error);
          });
    },
  }
</script>
