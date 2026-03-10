/**
 * Express router que monta las funciones asociadas a redes. 
 * @type {object}
 * @const
 * @namespace netRouter
 */
var debug = require('debug')('verbs:router')
var router = require('express').Router()
var regionCtrl = require('../controllers/spv3_controller')
var verbUtils = require('../controllers/verb_utils.js')

router.all('/', function(req, res) {
  res.json({ 
    data: { 
      message: '¡Yey! Bienvenido al API de SPECIES v3'
    }
  })
})

router.all('/db-health', async (req, res) => {
  try {
    var db = verbUtils.pool

    await db.one('SELECT 1 AS status')

    res.status(200).json({
      status: 'UP',
      message: 'database connected',
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    debug(error);
    res.status(503).json({
      status: 'DOWN',
      message: 'databse unreachable',
      error: error.message
    })
  }
})

router.route('/variables')
  .get(regionCtrl.variables)
  .post(regionCtrl.variables)

router.route('/secuencia')
  .get(regionCtrl.secuencia)
  .post(regionCtrl.secuencia)

router.route('/variables/:id')
  .get(regionCtrl.get_variable_byid)
  .post(regionCtrl.get_variable_byid)

router.route('/get-data/:id')
  .get(regionCtrl.get_data_byid)
  .post(regionCtrl.get_data_byid)

module.exports = router;
