<template>
  <v-card
    height="100%"
  >
    <v-navigation-drawer
      height="100%"
      v-model="drawer"
      :color="color"
      :expand-on-hover="expandOnHover"
      :mini-variant="miniVariant"
      :right="right"
      :permanent="permanent"
      absolute
      dark
      app
    >
      <v-list
        dense
        nav
        class="py-0"
      >
        <v-list-item two-line :class="miniVariant && 'px-0'">
          <v-list-item-content>
            <v-list-item-title>Results</v-list-item-title>
          </v-list-item-content>
        </v-list-item>

        <v-divider></v-divider>

        <v-list-item
          v-for="item in items"
          :key="item.title"
          link :to="item.href"
        >
          <v-list-item-icon>
            <v-icon>{{ item.icon }}</v-icon>
          </v-list-item-icon>

          <v-list-item-content>
            <v-list-item-title>{{ item.title }}</v-list-item-title>
          </v-list-item-content>
        </v-list-item>
      </v-list>
    </v-navigation-drawer>
    <v-content>
            <v-container class="px-4 py-0 fill-height" fluid>
              <router-view></router-view>
            </v-container>
        </v-content>
  </v-card>
</template>
<script>
export default {
   data: () => ({
      drawer: true,
      items: [
        { title: 'Analytics Dashboard', href: '/analytics', icon: 'mdi-view-dashboard' },
        { title: 'Comparison Dashboard', href:'/comparison', icon: 'mdi-view-dashboard'},
        { title: 'Recommendation System', href:'/recommend', icon: 'mdi-view-dashboard'},
        { title: 'Login Page', href:'/login', icon: 'mdi-view-dashboard'},
      ],
      color: 'dark',
      colors: [
        'primary',
        'blue',
        'success',
        'red',
        'teal',
      ],
      right: false,
      permanent: true,
      miniVariant: false,
      expandOnHover: false,
      background: false,
    }),
    components: {
  },

   
}
</script>
<style scoped>
.border {
  border-left: 4px solid #0ba518;
}
</style>
