import Vue from 'vue'
import App from './App.vue'
import vuetify from './plugins/vuetify';
import Router from 'vue-router';
Vue.config.productionTip = false
import Analytics from '@/components/Analytics.vue';
import Comparison from '@/components/Comparison.vue';
import Chartkick from 'vue-chartkick'
import Chart from 'chart.js'
import VCharts from 'v-charts'
import Login from '@/components/Login.vue'
import Recommend from '@/components/Recommend.vue'
import "@aws-amplify/ui-vue";
import Amplify from "aws-amplify";
import awsconfig from "./aws-exports";

Amplify.configure(awsconfig);
Vue.use(Chartkick.use(Chart))
Vue.use(Router)
import Axios from 'axios'
Vue.prototype.$http = Axios;
Vue.use(VCharts)

const router = new Router({
  mode: 'history',
  routes: [
    {
      path: '/',
      component: Login,
      meta: {},
      children: []
    },
    {
      path: '/login',
      component: Login,
      meta: {},
      children: []
    },
    {
      path: '/recommend',
      component: Recommend,
      meta: { requiresAuth: true},
      children: []
    },
    {
      path: '/analytics',
      component: Analytics,
      meta: { requiresAuth: true},
      children: []
    },
    {
      path: '/comparison',
      component: Comparison,
      meta: { requiresAuth: true},
      children: []
    },
  ]
});
router.beforeResolve((to, from, next) => {
  if (to.matched.some((record) => record.meta.requiresAuth)) {
    let user;
    Amplify.Auth.currentAuthenticatedUser()
      .then((data) => {
        if (data && data.signInUserSession) {
          user = data;
          console.log(user)
        }
        next();
      })
      .catch((e) => {
        alert(e)
        next({
          path: "/login",
        });
      });
  }
  next();
});
new Vue({
  vuetify,
  router: router,
  render: h => h(App)
}).$mount('#app')
