<template>
  <div>
    <amplify-authenticator username-alias="email">
	<h3> Welcome! </h3>
	<amplify-sign-out></amplify-sign-out>
    </amplify-authenticator>
  </div>
</template>

<script>
  export default {
    name: 'login',
    data: function() {
      return {
        email: '',
        password: '',
        user: null,
      }
    },
    methods: {
    },
  }
</script>
