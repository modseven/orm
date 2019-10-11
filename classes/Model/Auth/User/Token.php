<?php
/**
 * Default auth user toke
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) since 2016 Koseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\ORM\Model\Auth\User;

use KO7\Validation;
use Modseven\Database\DB;
use Modseven\ORM\Exception;
use Modseven\ORM\ORM;

class Token extends ORM
{
    /**
     * Belongs to
     * @var array
     */
    protected array $_belongs_to = [
        'user' => ['model' => 'User'],
    ];

    /**
     * Column created
     * @var mixed
     */
    protected $_created_column = [
        'column' => 'created',
        'format' => true,
    ];

    /**
     * Handles garbage collection and deleting of expired objects.
     *
     * @param mixed $id ID
     *
     * @throws Exception;
     * @throws \Exception;
     * @throws \KO7\Exception
     * @throws \Modseven\Database\Exception
     */
    public function __construct($id = null)
    {
        parent::__construct($id);

        if (random_int(1, 100) === 1) {
            // Do garbage collection
            $this->delete_expired();
        }

        if ($this->expires < time() && $this->_loaded) {
            // This object has expired
            $this->delete();
        }
    }

    /**
     * Deletes all expired tokens.
     *
     * @return  self
     *
     * @throws Exception
     */
    public function delete_expired() : self
    {
        // Delete all expired tokens
        try
        {
            DB::delete($this->_table_name)
                                 ->where('expires', '<', time())
                                 ->execute($this->_db);
        }
        catch (\KO7\Exception $e)
        {
            throw new Exception($e->getMessage(), null, $e->getCode(), $e);
        }


        return $this;
    }

    /**
     * @param Validation|null $validation
     *
     * @return ORM
     * @throws \Exception
     * @throws \Modseven\ORM\Validation\Exception
     */
    public function create(?Validation $validation = null) : ORM
    {
        $this->token = $this->create_token();

        return parent::create($validation);
    }

    /**
     * Create auth token
     *
     * @return string
     *
     * @throws \Exception
     */
    protected function create_token() : string
    {
        do
        {
            $token = sha1(uniqid(\KO7\Text::random('alnum', 32), true));
        } while (ORM::factory('User_Token', ['token' => $token])->loaded());

        return $token;
    }
}