<?php
/**
 * @package        KO7/ORM
 * even Team
 * @copyright  (c) 2016-2018 Koseven Team
 * @license        https://koseven.ga/LICENSE
 */
class ORM_Behavior_Guid extends ORM_Behavior {

	/**
	 * Table column for GUID value
	 * @var string
	 */
	protected $_guid_column = 'guid';

	/**
	 * Allow model creat on on guid key only
	 * @var boolean
	 */
	protected $_guid_only = TRUE;

	/**
	 * Verify GUID
	 * @var boolean
	 */
	protected $_guid_verify = FALSE;

	/**
	 * Constructs a behavior object
	 *
	 * @param   array $config Configuration parameters
	 */
	protected function __construct($config)
	{
		parent::__construct($config);

		$this->_guid_column = Arr::get($config, 'column', $this->_guid_column);
		$this->_guid_only = Arr::get($config, 'guid_only', $this->_guid_only);
		$this->_guid_verify = Arr::get($config, 'verify', $this->_guid_verify);
	}

	/**
	 * Constructs a new model and loads a record if given
	 *
	 * @param   ORM   $model The model
	 * @param   mixed $id    Parameter for find or object to load
	 *
	 * @return bool
	 * @throws KO7_Exception
	 */
	public function on_construct($model, $id) : bool
	{
		if (($id !== NULL) && ! is_array($id) && ! ctype_digit($id)) {
			if ( ! UUID:: valid($id)) {
				throw new KO7_Exception('Invalid UUID: :id', [':id' => $id]);
			}
			$model->where($this->_guid_column, '=', $id)->find();

			// Prevent further record loading
			return FALSE;
		}

		return TRUE;
	}

	/**
	 * The model is updated, add a guid value if empty
	 *
	 * @param   ORM $model The model
	 *
	 * @throws Exception
	 */
	public function on_update($model)
	{
		$this->create_guid($model);
	}

	/**
	 * Create GUUID
	 *
	 * @param $model	Model to generate GUID for
	 *
	 * @throws Exception
	 */
	private function create_guid($model)
	{
		if ($this->_guid_verify === FALSE)
		{
			$model->set($this->_guid_column, UUID::v4());
			return;
		}

		$current_guid = $model->get($this->_guid_column);

		// Try to create a new GUID
		$query = DB::select()->from($model->table_name())
			->where($this->_guid_column, '=', ':guid')
			->limit(1);

		while (empty($current_guid)) {
			$current_guid = UUID::v4();

			$query->param(':guid', $current_guid);
			if ($query->execute()->get($model->primary_key(), FALSE) !== FALSE) {
				Log::instance()->add(Log::NOTICE, 'Duplicate GUID created for '.$model->table_name());
				$current_guid = '';
			}
		}

		$model->set($this->_guid_column, $current_guid);
	}

	/**
	 * A new model is created, add a guid value
	 *
	 * @param   ORM $model The model
	 * @throws	Exception
	 */
	public function on_create($model)
	{
		$this->create_guid($model);
	}
}
