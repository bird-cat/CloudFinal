<template>
  <v-container fluid>
    <h1> Exection of analytics function comparison </h1>
    <v-card-text v-html="dialogText"></v-card-text>
    <v-divider></v-divider>
    <v-row>
        <v-col>
            <h2> {{name[0]}}</h2>
            <h3> Spark speeds up {{speedup[0]}} times</h3>
            <ve-histogram :data="chartData[0]" :extend="extend"></ve-histogram>
        </v-col>
        <v-col>
            <h2> {{name[1]}} </h2>
            <h3> Spark speeds up {{speedup[1]}} times</h3>
            <ve-histogram :data="chartData[1]" :extend="extend"></ve-histogram>
        </v-col>
        <v-col>
            <h2> {{name[2]}} </h2>
            <h3> Spark speeds up {{speedup[2]}} times</h3>
            <ve-histogram :data="chartData[2]" :extend="extend"></ve-histogram>
        </v-col>
    </v-row>
    <v-card-text v-html="dialogText"></v-card-text>
    <v-divider></v-divider>
    <v-row>
        <v-col>
            <h2> {{name[3]}} </h2>
            <h3> Spark speeds up {{speedup[3]}} times</h3>
            <ve-histogram :data="chartData[3]" :extend="extend"></ve-histogram>
        </v-col>
        <v-col>
            <h2> {{name[4]}} </h2>
            <h3> Spark speeds up {{speedup[4]}} times</h3>
            <ve-histogram :data="chartData[4]" :extend="extend"></ve-histogram>
        </v-col>
        <v-col>
            <h2> {{name[5]}} </h2>
            <h3> Spark speeds up {{speedup[5]}} times</h3>
            <ve-histogram :data="chartData[5]" :extend="extend"></ve-histogram>
        </v-col>
    </v-row>
    <v-card-text v-html="dialogText"></v-card-text>
    <v-divider></v-divider>
    <v-row>
        <v-col>
            <h2> {{name[6]}} </h2>
            <h3> Spark speeds up {{speedup[6]}} times</h3>
            <ve-histogram :data="chartData[6]" :extend="extend"></ve-histogram>
        </v-col>
        <v-col>
            <h2> {{name[7]}} </h2>
            <h3> Spark speeds up {{speedup[7]}} times</h3>
            <ve-histogram :data="chartData[7]" :extend="extend"></ve-histogram>
        </v-col>
        <v-col>
            <h2> {{name[8]}} </h2>
            <h3> Spark speeds up {{speedup[8]}} times</h3>
            <ve-histogram :data="chartData[8]" :extend="extend"></ve-histogram>
        </v-col>
    </v-row>
  </v-container>
</template>

<script>
  export default {
    name: 'Comparison',

    data: () => ({
        extend: {
          series: {
            label: { show: true, position: "top" }
          }
        },
        dialogText: "<br>",
        chartData: [
        ],
        speedup: [],
        name: [],
    }),
    mounted() {
        var vm = this;
        this.$http.get('[Your API Host], {headers: {
          'Access-Control-Allow-Origin': '*'
           },})
          .then(function (response) {
              vm.chartData = response.data.data
              console.log(vm.chartData[0])
              vm.speedup = response.data.speedup
              vm.name = response.data.name
          })
          .catch(function (error) {
              console.log(error);
          });
    },
  }
</script>
